<#
.SYNOPSIS
    Deploys FabricDaxLoadTest into an existing Fabric workspace under a
    `LoadTests` workspace folder.

.DESCRIPTION
    Creates (or updates, idempotently) the following items in the target
    workspace, all inside a workspace folder named `LoadTests`:

      * LoadTests.Lakehouse  — holds Files/fdlt_runtime-<ver>.whl
                                (the fat wheel: LoadGen.dll + ADOMD
                                assemblies bundled inside the Python
                                wheel as of v0.5.0), Files/runs/<runId>/
                                (per-run telemetry), and
                                Tables/dbo/LoadTest{s,Runs,Queries,QueryExecutions,TraceEvents}.
      * LoadTest - Main      — the runner notebook; users edit cell 1 and
                                Run All directly. Save As (in the portal)
                                to make *additional* Load Tests in the
                                same workspace. Redeploys refresh the
                                wheel and rebake cell 2 (WHEEL_URL +
                                wheel filename) on the existing notebook
                                so it stays in sync with what's in
                                Files/. User edits to cell 1 are
                                preserved (cell 1 is a parameters cell;
                                rebake only touches cell 2).

    Everything else is idempotent — re-run any time to refresh the wheel.
    Folder and lakehouse names are always `LoadTests`; the notebook
    auto-discovers the lakehouse from the workspace.

.PARAMETER Workspace
    Display name of the target workspace.

.PARAMETER SkipPublish
    Skip `dotnet publish`; reuse the existing publish output under
    src/LoadGen/bin/Release/net8.0/linux-x64/publish/.

.PARAMETER SkipNotebookUpdate
    Leave an existing `LoadTest - Main` notebook in the workspace
    untouched, even if its embedded WHEEL_URL points at an older wheel.
    Default behavior is to redeploy the notebook with the freshly
    baked WHEEL_URL so it stays in sync with the wheel that was just
    uploaded to Files/. Use this only when you've made manual edits to
    cells 2-onwards in the portal that you don't want clobbered (cell
    1 is the parameters cell — those edits live in `LoadTest - <Name>`
    Save-As copies, never in `LoadTest - Main`).

.EXAMPLE
    pwsh scripts\Deploy-LoadTests.ps1 -Workspace dbrowne-loadtest -Verbose
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Workspace,
    [switch] $SkipPublish,
    [switch] $SkipNotebookUpdate
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ApiBase  = "https://api.fabric.microsoft.com"
$Resource = $ApiBase
$RepoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$PublishDir = Join-Path $RepoRoot "src\LoadGen\bin\Release\net8.0\linux-x64\publish"
$NotebooksDir = Join-Path $RepoRoot "notebooks"

# ---- helpers ----------------------------------------------------------------
function Step([string]$msg) { Write-Host "==> $msg" -ForegroundColor Cyan }
function Info([string]$msg) { Write-Host "    $msg" -ForegroundColor DarkGray }
function Warn([string]$msg) { Write-Host "    $msg" -ForegroundColor Yellow }

function Invoke-Fabric {
    param(
        [Parameter(Mandatory)] [ValidateSet("GET","POST","DELETE","PATCH","PUT")] [string]$Method,
        [Parameter(Mandatory)] [string]$Path,
        [object] $Body
    )
    $url  = "$ApiBase$Path"
    $args = @("rest","--resource",$Resource,"--method",$Method.ToLower(),"--url",$url)
    if ($PSBoundParameters.ContainsKey("Body")) {
        $tmp = New-TemporaryFile
        ($Body | ConvertTo-Json -Depth 12 -Compress) | Out-File $tmp -Encoding utf8 -NoNewline
        $args += @("--body","@$($tmp.FullName)","--headers","Content-Type=application/json")
    }
    $raw = & az @args 2>&1
    $exit = $LASTEXITCODE
    if ($exit -ne 0) {
        throw "Fabric $Method $Path failed (exit=$exit):`n$raw"
    }
    if (-not $raw) { return $null }
    return ($raw -join "`n" | ConvertFrom-Json)
}

function Get-FabricToken {
    $tok = & az account get-access-token --resource $Resource --query accessToken -o tsv 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Token acquire failed: $tok" }
    return $tok.Trim()
}

function Invoke-FabricRaw {
    # Invoke a Fabric REST call that needs response headers (e.g. LRO Location).
    # Returns @{ StatusCode; Headers; Body (parsed JSON or $null) }
    param(
        [Parameter(Mandatory)] [string]$Method,
        [Parameter(Mandatory)] [string]$Url,
        [object] $Body
    )
    $tok = Get-FabricToken
    $headers = @{ Authorization = "Bearer $tok" }
    $params = @{
        Method = $Method
        Uri = $Url
        Headers = $headers
        ResponseHeadersVariable = "respHeaders"
        SkipHttpErrorCheck = $true
        StatusCodeVariable = "statusCode"
    }
    if ($PSBoundParameters.ContainsKey("Body")) {
        $params.Body = ($Body | ConvertTo-Json -Depth 20 -Compress)
        $params.ContentType = "application/json"
    }
    $resp = Invoke-RestMethod @params
    if ($statusCode -ge 400) {
        throw "$Method $Url failed (HTTP $statusCode): $($resp | ConvertTo-Json -Depth 6 -Compress)"
    }
    return @{ StatusCode = $statusCode; Headers = $respHeaders; Body = $resp }
}

function Wait-LRO([string]$Location) {
    while ($true) {
        $r = Invoke-FabricRaw -Method GET -Url $Location
        $obj = $r.Body
        if ($obj.status -in @("Succeeded","Failed","Cancelled")) {
            if ($obj.status -ne "Succeeded") {
                throw "LRO ended with status $($obj.status): $($obj | ConvertTo-Json -Depth 6)"
            }
            $resp = Invoke-FabricRaw -Method GET -Url "$Location/result"
            return $resp.Body
        }
        Start-Sleep -Seconds 2
    }
}

function ConvertTo-B64File([string]$Path) {
    return [Convert]::ToBase64String([IO.File]::ReadAllBytes($Path))
}

function ConvertTo-B64String([string]$Text) {
    return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Text))
}

# ---- preflight --------------------------------------------------------------
Step "Preflight"
$null = Get-Command az -ErrorAction Stop
$null = Get-Command fab -ErrorAction Stop
Info "az + fab CLIs found"

if (-not $SkipPublish) {
    Step "dotnet publish LoadGen (Release, linux-x64, framework-dependent)"
    # Notebook deployment runs `dotnet LoadGen.dll` on Fabric Spark
    # nodes — Linux only, .NET 8 runtime already present via sempy.
    # Framework-dependent (no SC) keeps the zip payload at ~3.5 MB
    # instead of ~67 MB; UseAppHost=false skips the apphost binary so we
    # don't need a chmod step after OneLake stages the files (and
    # cross-platform the same DLLs run unchanged).
    & dotnet publish (Join-Path $RepoRoot "src\LoadGen\LoadGen.csproj") `
        -c Release -r linux-x64 `
        -p:SelfContained=false -p:PublishSingleFile=false -p:UseAppHost=false `
        --nologo -v minimal | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed" }
}
if (-not (Test-Path (Join-Path $PublishDir "LoadGen.dll"))) {
    throw "Publish output not found: $PublishDir. Re-run without -SkipPublish."
}
$published = Get-ChildItem -Recurse -File $PublishDir
$totalKb = [int](( $published | Measure-Object Length -Sum).Sum / 1024)
Info "Publish output: $($published.Count) files, $totalKb KiB"

Step "Staging .NET LoadGen binaries into the wheel source tree"
# As of v0.5.0 the LoadGen DLLs ship inside the fdlt_runtime wheel
# (under fdlt_runtime/loadgen/). Pre-populate that folder from the
# fresh `dotnet publish` output so the next `python -m build` picks
# them up via [tool.setuptools.package-data]. The folder is gitignored
# (build output, not source) so a stale copy from a prior deploy is
# wiped first.
$LoadgenStage = Join-Path $RepoRoot "src\fdlt_runtime\loadgen"
if (Test-Path $LoadgenStage) { Remove-Item $LoadgenStage -Recurse -Force }
New-Item -ItemType Directory -Path $LoadgenStage | Out-Null
Copy-Item -Path (Join-Path $PublishDir "*") -Destination $LoadgenStage -Recurse
$staged = Get-ChildItem -Recurse -File $LoadgenStage
$stagedKb = [int](( $staged | Measure-Object Length -Sum).Sum / 1024)
Info "Staged $($staged.Count) files, $stagedKb KiB into src/fdlt_runtime/loadgen/"

# Sanity check: the files we depend on at runtime had better be there.
$requiredStage = @(
    "LoadGen.dll", "LoadGen.deps.json", "LoadGen.runtimeconfig.json",
    "QueryRunner.dll", "Microsoft.AnalysisServices.AdomdClient.dll"
)
foreach ($f in $requiredStage) {
    if (-not (Test-Path (Join-Path $LoadgenStage $f))) {
        throw "Staging missing required file: $f. dotnet publish output may be stale."
    }
}

# setuptools-scm derives the version from `git describe`, so an
# explicit tag (or dirty-tree dev marker) drives both the wheel
# filename and the runtime banner.
Step "Building fdlt_runtime wheel (with bundled LoadGen binaries)"
$DistDir = Join-Path $RepoRoot "dist"
if (Test-Path $DistDir) {
    # Clear stale wheels so we don't ship two side-by-side.
    Get-ChildItem $DistDir -Filter "fdlt_runtime-*.whl" -ErrorAction SilentlyContinue |
        Remove-Item -Force
}
Push-Location $RepoRoot
try {
    & python -m build --wheel --no-isolation 2>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "python -m build failed" }
} finally { Pop-Location }
$wheel = Get-ChildItem $DistDir -Filter "fdlt_runtime-*.whl" |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $wheel) { throw "Wheel build produced no fdlt_runtime-*.whl" }
$wheelKb = [int]($wheel.Length / 1024)
Info "Wheel: $($wheel.Name) ($wheelKb KiB)"

# Verify the wheel actually bundled the LoadGen binaries — the
# package-data glob is easy to silently break and a stale ~50 KiB
# wheel ships fine but fails at the user's first Run-All.
$verifyDir = Join-Path $env:TEMP "fdlt-wheel-verify"
if (Test-Path $verifyDir) { Remove-Item $verifyDir -Recurse -Force }
Expand-Archive -Path $wheel.FullName -DestinationPath $verifyDir
$mustExist = @(
    "fdlt_runtime/loadgen/LoadGen.dll",
    "fdlt_runtime/loadgen/LoadGen.deps.json",
    "fdlt_runtime/loadgen/LoadGen.runtimeconfig.json",
    "fdlt_runtime/loadgen/QueryRunner.dll",
    "fdlt_runtime/loadgen/Microsoft.AnalysisServices.AdomdClient.dll",
    "fdlt_runtime/notebook.py"
)
foreach ($rel in $mustExist) {
    $p = Join-Path $verifyDir ($rel -replace '/', '\')
    if (-not (Test-Path $p)) {
        throw "Built wheel is missing $rel. Check pyproject.toml package-data + the loadgen/ staging step."
    }
}
Remove-Item $verifyDir -Recurse -Force
Info "Wheel content verified (LoadGen + dependencies bundled)"

Step "Rebuilding notebook (LoadTest-Main.ipynb)"
# Build with a sentinel WHEEL_URL — we patch it below to the abfss://
# path of the freshly-uploaded wheel after we know the lakehouse GUID.
Push-Location $RepoRoot
try {
    $env:FDLT_RELEASE_VERSION = ""
    & python (Join-Path $RepoRoot "scripts\build_notebooks.py") | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "build_notebooks.py failed" }
} finally { Pop-Location }

# ---- resolve workspace ------------------------------------------------------
Step "Resolving workspace '$Workspace'"
$wsList = Invoke-Fabric GET "/v1/workspaces"
$ws = $wsList.value | Where-Object { $_.displayName -eq $Workspace }
if (-not $ws) {
    throw "Workspace '$Workspace' not found. Available: $($wsList.value.displayName -join ', ')"
}
$wsId = $ws.id
Info "WorkspaceId: $wsId"

# ---- folder -----------------------------------------------------------------
Step "Get-or-create folder 'LoadTests'"
$folders = Invoke-Fabric GET "/v1/workspaces/$wsId/folders"
$folder = $folders.value | Where-Object {
    $_.displayName -eq "LoadTests" -and -not ($_.PSObject.Properties.Name -contains 'parentFolderId')
}
if (-not $folder) {
    $folder = Invoke-Fabric POST "/v1/workspaces/$wsId/folders" @{ displayName = "LoadTests" }
    Info "Created folder $($folder.id)"
} else {
    Info "Reusing folder $($folder.id)"
}
$folderId = $folder.id

# ---- lakehouse --------------------------------------------------------------
Step "Get-or-create lakehouse 'LoadTests' (schema-enabled)"
$items = Invoke-Fabric GET "/v1/workspaces/$wsId/items?type=Lakehouse"
$lh = $items.value | Where-Object { $_.displayName -eq "LoadTests" }
if (-not $lh) {
    # creationPayload.enableSchemas = $true opts into the schema-preview
    # layout (Tables/dbo/<name>) so the notebook's Tables/dbo/* writes
    # land in the canonical place. The notebook also handles flat
    # lakehouses, but new deployments should always be schema-enabled.
    # Schema-enabled creation is async (HTTP 202 + Location header); use
    # Invoke-FabricRaw so we can poll the LRO to completion.
    $body = @{
        displayName     = "LoadTests"
        type            = "Lakehouse"
        folderId        = $folderId
        creationPayload = @{ enableSchemas = $true }
    }
    $r = Invoke-FabricRaw -Method POST -Url "$ApiBase/v1/workspaces/$wsId/items" -Body $body
    if ($r.StatusCode -eq 202) {
        $loc = $r.Headers["Location"]
        if ($loc -is [array]) { $loc = $loc[0] }
        Info "Create returned 202 LRO; polling..."
        $lh = Wait-LRO $loc
    } else {
        $lh = $r.Body
    }
    Info "Created schema-enabled lakehouse $($lh.id)"
} else {
    if ($lh.folderId -ne $folderId) {
        Warn "Lakehouse exists but is in a different folder ($($lh.folderId) vs $folderId). Leaving in place."
    } else {
        Info "Reusing lakehouse $($lh.id)"
    }
    # Confirm the reused lakehouse is schema-enabled. We can't flip the
    # bit retroactively (Fabric API has no "convert to schemas" path), so
    # warn the user — the notebook will fall back to flat-Tables writes.
    try {
        $lhDetail = Invoke-Fabric GET "/v1/workspaces/$wsId/lakehouses/$($lh.id)"
        $defaultSchema = $lhDetail.properties.defaultSchema
        if ($defaultSchema) {
            Info "Lakehouse is schema-enabled (defaultSchema=$defaultSchema)"
        } else {
            Warn "Lakehouse is NOT schema-enabled — notebook will write to Tables/ instead of Tables/dbo/."
            Warn "  To migrate: delete the existing LoadTests lakehouse from the workspace and re-run this script."
        }
    } catch {
        Warn "Could not read lakehouse properties to verify schema-enabled state: $($_.Exception.Message)"
    }
}
$lhId = $lh.id

# ---- upload fdlt_runtime wheel via fab cp -----------------------------------
Step "Uploading $($wheel.Name) to LoadTests.Lakehouse/Files/"
$lhWheelDest = "/$Workspace.workspace/LoadTests.lakehouse/Files/$($wheel.Name)"
& fab cp $wheel.FullName $lhWheelDest -f 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw "fab cp failed for $($wheel.FullName) -> $lhWheelDest" }
Info "Uploaded $($wheel.Name) ($wheelKb KiB)"

# Build the abfss:// URL the notebook will pull from. Friendly-name
# paths (`abfss://…/LoadTests.Lakehouse/…`) are disabled on some
# tenants; GUID paths always work.
$WheelAbfssUrl = "abfss://$wsId@onelake.dfs.fabric.microsoft.com/$lhId/Files/$($wheel.Name)"
Info "Wheel ABFSS URL: $WheelAbfssUrl"

# Patch the generated notebook's WHEEL_URL to point at the freshly
# uploaded wheel. build_notebooks.py emits the sentinel
# `REPLACE_ME_WITH_WHEEL_URL`; we swap it for the abfss:// URL here so
# every redeploy lines up cell 2 with whatever filename setuptools-scm
# minted for this build.
Step "Patching notebook WHEEL_URL"
$NotebookPath = Join-Path $NotebooksDir "LoadTest-Main.ipynb"
$nbText = Get-Content $NotebookPath -Raw
if ($nbText -notmatch 'REPLACE_ME_WITH_WHEEL_URL') {
    throw "Notebook does not contain the WHEEL_URL sentinel — did build_notebooks.py change?"
}
# Notebook source is JSON, and the cell-source string carries the URL
# as a plain literal — straight string replace is safe (the sentinel
# appears nowhere else).
$nbText = $nbText.Replace('REPLACE_ME_WITH_WHEEL_URL', $WheelAbfssUrl)
Set-Content -Path $NotebookPath -Value $nbText -Encoding UTF8 -NoNewline
Info "Patched WHEEL_URL into $NotebookPath"

# Best-effort cleanup of legacy artifacts from pre-v0.5.0 deploys.
# The old loadgen-bin.zip and Files/bin/ tree are no longer used —
# leave them in place by default (cleaning them could break a
# pre-upgrade saved notebook the user hasn't migrated yet), but log
# a hint so users know they can delete by hand.
$legacyZip = "/$Workspace.workspace/LoadTests.lakehouse/Files/loadgen-bin.zip"
$legacyZipExists = & fab exists $legacyZip 2>&1 | Select-String -Pattern '"data": true' -Quiet
if ($legacyZipExists) {
    Warn "Legacy Files/loadgen-bin.zip still present (from a pre-v0.5.0 deploy)."
    Warn "  Safe to delete once all `LoadTest - …` notebooks have been redeployed/regenerated."
}
$legacyBin = "/$Workspace.workspace/LoadTests.lakehouse/Files/bin"
$legacyExists = & fab exists $legacyBin 2>&1 | Select-String -Pattern '"data": true' -Quiet
if ($legacyExists) {
    Info "Removing legacy Files/bin/ tree"
    & fab rm -r $legacyBin -f 2>&1 | Out-Null
}

# ---- create/update notebook via REST (folderId at create time) --------------
function Deploy-Notebook {
    param(
        [Parameter(Mandatory)] [string]$Name,
        [Parameter(Mandatory)] [string]$IpynbPath,
        [string]$Description = ""
    )
    Step "Deploying notebook '$Name'"
    if (-not (Test-Path $IpynbPath)) { throw "Notebook source missing: $IpynbPath" }

    $platformJson = @{
        '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json'
        metadata  = @{ type = "Notebook"; displayName = $Name; description = $Description }
        config    = @{ version = "2.0"; logicalId = "00000000-0000-0000-0000-000000000000" }
    } | ConvertTo-Json -Depth 6 -Compress

    $definition = @{
        format = "ipynb"
        parts  = @(
            @{ path = "notebook-content.ipynb"; payload = (ConvertTo-B64File $IpynbPath); payloadType = "InlineBase64" }
            @{ path = ".platform";               payload = (ConvertTo-B64String $platformJson); payloadType = "InlineBase64" }
        )
    }

    # Look up existing notebook
    $existingItems = Invoke-Fabric GET "/v1/workspaces/$wsId/items?type=Notebook"
    $existing = $existingItems.value | Where-Object { $_.displayName -eq $Name -and $_.folderId -eq $folderId }

    if (-not $existing) {
        $body = @{ displayName = $Name; description = $Description; folderId = $folderId; definition = $definition }
        $url = "$ApiBase/v1/workspaces/$wsId/notebooks"
        $r = Invoke-FabricRaw -Method POST -Url $url -Body $body
        if ($r.StatusCode -eq 202) {
            $loc = $r.Headers["Location"]
            if ($loc -is [array]) { $loc = $loc[0] }
            Info "Create returned 202 LRO; polling..."
            $created = Wait-LRO $loc
            Info "  Created id=$($created.id)"
        } else {
            Info "Create returned $($r.StatusCode) (synchronous), id=$($r.Body.id)"
        }
    } elseif ($SkipNotebookUpdate) {
        Info "Found existing notebook id=$($existing.id) — leaving in place (-SkipNotebookUpdate)"
        Warn "  Cell 2 still points at whatever WHEEL_URL was embedded the last time the notebook was deployed."
        Warn "  If that wheel is no longer in Files/, the notebook will fail at the pip-install step."
    } else {
        Info "Reusing notebook id=$($existing.id) — updating definition (WHEEL_URL is rebaked each deploy)"
        $body = @{ definition = $definition }
        $url = "$ApiBase/v1/workspaces/$wsId/notebooks/$($existing.id)/updateDefinition"
        $r = Invoke-FabricRaw -Method POST -Url $url -Body $body
        if ($r.StatusCode -eq 202) {
            $loc = $r.Headers["Location"]
            if ($loc -is [array]) { $loc = $loc[0] }
            Info "Update returned 202 LRO; polling..."
            $null = Wait-LRO $loc
        } else {
            Info "Update returned $($r.StatusCode) (synchronous)"
        }
    }
}

Deploy-Notebook -Name "LoadTest - Main" -IpynbPath (Join-Path $NotebooksDir "LoadTest-Main.ipynb") `
                -Description "FabricDaxLoadTest runner — edit cell 1 and Run All."

# ---- summary ----------------------------------------------------------------
Step "Done"
Write-Host ""
Write-Host "Workspace : $Workspace ($wsId)" -ForegroundColor Green
Write-Host "Folder    : LoadTests ($folderId)" -ForegroundColor Green
Write-Host "Lakehouse : LoadTests.Lakehouse ($lhId)" -ForegroundColor Green
Write-Host "Notebooks : LoadTest - Main" -ForegroundColor Green
Write-Host ""
Write-Host "Minimal use case:" -ForegroundColor White
Write-Host "  1. Open 'LoadTest - Main' in the workspace." -ForegroundColor White
Write-Host "  2. Drag a Power BI Desktop Performance Analyzer .json onto the" -ForegroundColor White
Write-Host "     notebook's Resources panel (left sidebar)." -ForegroundColor White
Write-Host "  3. Edit TARGET_DATASET in cell 1 (or leave None to auto-pick" -ForegroundColor White
Write-Host "     the only model in the workspace) and Run All." -ForegroundColor White
Write-Host ""
Write-Host "Save As 'LoadTest - <name>' to add additional Load Tests." -ForegroundColor White
Write-Host "https://app.powerbi.com/groups/$wsId/list" -ForegroundColor White
