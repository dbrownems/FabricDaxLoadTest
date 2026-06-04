<#
.SYNOPSIS
    Deploys FabricDaxLoadTest into an existing Fabric workspace under a
    `LoadTests` workspace folder.

.DESCRIPTION
    Creates (or updates, idempotently) the following items in the target
    workspace, all inside a workspace folder named `LoadTests`:

      * LoadTests.Lakehouse  — holds Files/bin/QueryRunner.dll + dependencies,
                                Files/queries.json (DAX corpus),
                                Files/runs/<runId>/ (per-run telemetry).
      * Run.Notebook         — the runner; auto-discovers the lakehouse and
                                drives concurrent DAX queries.
      * Queries.Notebook     — read/edit Files/queries.json.

    Everything is idempotent — re-run any time to refresh assemblies or
    notebook content.

.PARAMETER Workspace
    Display name of the target workspace.

.PARAMETER FolderName
    Workspace-folder name. Default: 'LoadTests'.

.PARAMETER LakehouseName
    Lakehouse display name. Default: 'LoadTests'.

.PARAMETER SkipPublish
    Skip `dotnet publish`; reuse the existing publish output under
    src/QueryRunner/bin/Release/net8.0/publish/.

.PARAMETER SkipNotebooks
    Skip rebuilding notebooks/Run.ipynb + notebooks/Queries.ipynb. The
    deployed notebooks always come from the most-recent files on disk.

.EXAMPLE
    pwsh scripts\Deploy-LoadTests.ps1 -Workspace dbrowne-loadtest -Verbose
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Workspace,
    [string] $FolderName    = "LoadTests",
    [string] $LakehouseName = "LoadTests",
    [switch] $SkipPublish,
    [switch] $SkipNotebooks
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ApiBase  = "https://api.fabric.microsoft.com"
$Resource = $ApiBase
$RepoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$PublishDir = Join-Path $RepoRoot "src\QueryRunner\bin\Release\net8.0\publish"
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
    Step "dotnet publish QueryRunner (Release)"
    & dotnet publish (Join-Path $RepoRoot "src\QueryRunner\QueryRunner.csproj") -c Release --nologo -v minimal | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed" }
}
if (-not (Test-Path (Join-Path $PublishDir "QueryRunner.dll"))) {
    throw "Publish output not found: $PublishDir. Re-run without -SkipPublish."
}
$published = Get-ChildItem -Recurse -File $PublishDir
$totalKb = [int](( $published | Measure-Object Length -Sum).Sum / 1024)
Info "Publish output: $($published.Count) files, $totalKb KiB"

if (-not $SkipNotebooks) {
    Step "Rebuilding notebooks (Run.ipynb + Queries.ipynb)"
    Push-Location $RepoRoot
    try {
        & python (Join-Path $RepoRoot "scripts\build_notebooks.py") | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "build_notebooks.py failed" }
    } finally { Pop-Location }
}

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
Step "Get-or-create folder '$FolderName'"
$folders = Invoke-Fabric GET "/v1/workspaces/$wsId/folders"
$folder = $folders.value | Where-Object {
    $_.displayName -eq $FolderName -and -not ($_.PSObject.Properties.Name -contains 'parentFolderId')
}
if (-not $folder) {
    $folder = Invoke-Fabric POST "/v1/workspaces/$wsId/folders" @{ displayName = $FolderName }
    Info "Created folder $($folder.id)"
} else {
    Info "Reusing folder $($folder.id)"
}
$folderId = $folder.id

# ---- lakehouse --------------------------------------------------------------
Step "Get-or-create lakehouse '$LakehouseName'"
$items = Invoke-Fabric GET "/v1/workspaces/$wsId/items?type=Lakehouse"
$lh = $items.value | Where-Object { $_.displayName -eq $LakehouseName }
if (-not $lh) {
    $body = @{ displayName = $LakehouseName; type = "Lakehouse"; folderId = $folderId }
    $lh = Invoke-Fabric POST "/v1/workspaces/$wsId/items" $body
    Info "Created lakehouse $($lh.id)"
} else {
    if ($lh.folderId -ne $folderId) {
        Warn "Lakehouse exists but is in a different folder ($($lh.folderId) vs $folderId). Leaving in place."
    } else {
        Info "Reusing lakehouse $($lh.id)"
    }
}
$lhId = $lh.id

# ---- upload assemblies via fab cp ------------------------------------------
Step "Uploading assemblies to LoadTests.Lakehouse/Files/bin"
$lhBin = "/$Workspace.workspace/$LakehouseName.lakehouse/Files/bin"
& fab mkdir "$lhBin" 2>&1 | Out-Null
$createdDirs = @{ $lhBin = $true }
foreach ($f in $published) {
    $rel = $f.FullName.Substring($PublishDir.Length).TrimStart('\','/').Replace('\','/')
    $destPath = "$lhBin/$rel"
    # Ensure every ancestor directory exists (fab mkdir is one-level-only).
    $segments = ($rel -split '/')
    for ($i = 0; $i -lt $segments.Count - 1; $i++) {
        $dir = "$lhBin/" + ($segments[0..$i] -join '/')
        if (-not $createdDirs.ContainsKey($dir)) {
            & fab mkdir $dir 2>&1 | Out-Null
            $createdDirs[$dir] = $true
        }
    }
    & fab cp $f.FullName $destPath -f 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "fab cp failed for $($f.FullName) -> $destPath" }
}
Info "Uploaded $($published.Count) files"

# ---- seed empty queries.json if absent --------------------------------------
$qPath = "/$Workspace.workspace/$LakehouseName.lakehouse/Files/queries.json"
$qExists = & fab exists $qPath 2>&1 | Select-String -Pattern '"data": true' -Quiet
if (-not $qExists) {
    Step "Seeding empty queries.json"
    $tmp = New-TemporaryFile
    '["EVALUATE ROW(\"x\", 1)"]' | Out-File $tmp -Encoding utf8 -NoNewline
    & fab cp $tmp.FullName $qPath -f 2>&1 | Out-Null
    Remove-Item $tmp -Force
}

# ---- create/update notebooks via REST (folderId at create time) -------------
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
    } else {
        Info "Reusing notebook id=$($existing.id) — updating definition"
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

Deploy-Notebook -Name "Run"     -IpynbPath (Join-Path $NotebooksDir "Run.ipynb")     `
                -Description "FabricDaxLoadTest runner — drives concurrent DAX queries via XMLA."
Deploy-Notebook -Name "Queries" -IpynbPath (Join-Path $NotebooksDir "Queries.ipynb") `
                -Description "FabricDaxLoadTest query catalog editor (Files/queries.json)."

# ---- summary ----------------------------------------------------------------
Step "Done"
Write-Host ""
Write-Host "Workspace : $Workspace ($wsId)" -ForegroundColor Green
Write-Host "Folder    : $FolderName ($folderId)" -ForegroundColor Green
Write-Host "Lakehouse : $LakehouseName.Lakehouse ($lhId)" -ForegroundColor Green
Write-Host "Notebooks : Run, Queries" -ForegroundColor Green
Write-Host ""
Write-Host "Open the workspace and run the 'Run' notebook to start a load test." -ForegroundColor White
Write-Host "https://app.powerbi.com/groups/$wsId/list" -ForegroundColor White
