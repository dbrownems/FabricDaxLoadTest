# FabricDaxLoadTest

A load testing tool for Microsoft Fabric and Power BI semantic models. Simulates concurrent users executing DAX queries against the **XMLA endpoint** using ADOMD.NET, then lands per-query telemetry in Delta tables for analysis in Power BI.

Designed to run **inside a Fabric notebook** — no Spark cluster, no separate VM, no `dotnet build` required for end users. A deploy script (or a few manual portal steps) drops a `LoadTest - Template` notebook + `LoadTests` lakehouse into your workspace; users **Save As** the template per test and run.

> ⚠️ **Active rewrite (2026 H1).** This branch replaces the original
> [FabricLoadTestTool](https://github.com/microsoft/fabric-toolbox/tree/main/tools/FabricLoadTestTool).
> Some sections of this README describe surface area that is still in
> flux — see [Status](#status) for the current authoritative scope.

## Why another load test tool?

The existing pure-Python REST-based tools have meaningful limitations:

- No realistic per-user XMLA connection cost (TCP/TLS handshake, model attach).
- Limited to whatever the REST API path exposes.
- Can't easily simulate Row-Level Security via `EffectiveUsername` / `Roles`.
- Per-thread Python overhead caps achievable concurrency.

This tool drives **ADOMD.NET** out-of-process, so each simulated user gets a real XMLA connection — the same path Power BI Desktop, Excel, and Tabular Editor use. That makes the test results meaningful for capacity planning.

## Components

| Piece | What it is |
|---|---|
| `QueryRunner.dll` | .NET 8 library: orchestrates concurrent users, opens ADOMD.NET connections, runs DAX, writes per-query telemetry CSV. |
| `LoadGen.dll` / `LoadGen.exe` | Thin .NET CLI wrapper over `QueryRunner`. Run as `dotnet LoadGen.dll …` from the notebook (Linux Spark host) or as `LoadGen.exe` locally. |
| `notebooks/Run.ipynb` | Deployed as **`LoadTest - Template`**. Save-As → edit cell 1 → Run All. |
| `notebooks/Queries.ipynb` | Deployed as `Queries`. Editor for the lakehouse `Files/queries.json` corpus. |
| `scripts/Deploy-LoadTests.ps1` | One-shot deploy: builds LoadGen, creates the folder + lakehouse, uploads bits, deploys both notebooks. |

## Status

- ✅ Notebook-driven DAX load tests against any Fabric/PBI semantic model via XMLA.
- ✅ Per-run telemetry CSV under `Files/runs/<runId>/`.
- ✅ Delta tables (`LoadTests`, `LoadTestRuns`, `LoadTestQueries`, `LoadTestQueryExecutions`) written from the notebook for Power BI Direct Lake reporting.
- 🚧 AS-trace capture during a run, monitor mode against an external model, and the load-test-from-trace extractor — designed in `plan.md`, not yet implemented.

---

## Setup

You need a Fabric workspace on capacity that can host a Lakehouse + Notebooks, and an account with **Member** or **Admin** access to that workspace. The semantic model you want to load-test must be reachable via its XMLA endpoint, and your account needs **Build** permission on the model.

The tool deploys a single self-contained bundle into a workspace folder:

```
<your-workspace>/
└── LoadTests/                         ← workspace folder
    ├── LoadTests (Lakehouse)
    │   ├── Files/
    │   │   ├── bin/                   ← LoadGen.dll + ADOMD client deps
    │   │   ├── queries.json           ← DAX corpus
    │   │   └── runs/<RunId>/          ← per-run telemetry CSVs
    │   └── Tables/dbo/
    │       ├── LoadTests
    │       ├── LoadTestRuns
    │       ├── LoadTestQueries
    │       └── LoadTestQueryExecutions
    ├── LoadTest - Template (Notebook) ← Save-As to start each run
    └── Queries (Notebook)
```

Everything (lakehouse, notebooks, files) lives inside the `LoadTests` workspace folder so the notebook can self-discover the lakehouse from its own runtime context.

### Option A — Scripted (recommended)

Prerequisites:

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- [Azure CLI (`az`)](https://learn.microsoft.com/cli/azure/install-azure-cli) — `az login` to a tenant where your account has the workspace permissions above.
- [Fabric CLI (`fab`)](https://learn.microsoft.com/fabric/cli/install) — `fab auth login` (used for fast OneLake file uploads).
- PowerShell 7+ (`pwsh`).
- A clone of this repo.

```pwsh
git clone https://github.com/dbrownems/FabricDaxLoadTest.git
cd FabricDaxLoadTest

az login
fab auth login

pwsh ./scripts/Deploy-LoadTests.ps1 -Workspace "<your-workspace-display-name>" -Verbose
```

The script is fully idempotent — re-run any time to refresh the LoadGen bits or notebook content. The deploy creates or updates:

- the `LoadTests` workspace folder
- `LoadTests.Lakehouse`
- `LoadTest - Template` and `Queries` notebooks
- `Files/bin/*` and a default `Files/queries.json`

Useful flags:

| Flag | Effect |
|---|---|
| `-FolderName "X"` | Use a different folder name (default `LoadTests`). |
| `-LakehouseName "X"` | Use a different lakehouse name (default `LoadTests`). |
| `-SkipPublish` | Skip `dotnet publish`; reuse the existing publish output. |
| `-SkipNotebooks` | Skip `build_notebooks.py`; deploy whatever `notebooks/*.ipynb` already exists on disk. |

> **Migration from earlier deploys.** The script renames any pre-existing
> `Run` notebook in the `LoadTests` folder to `LoadTest - Template`
> automatically (the user's customizations would have been wiped on every
> redeploy anyway — this just normalizes the new name).

### Option B — Manual setup

If you can't run the deploy script (no local CLIs, restricted network, etc.), you can build the layout above by hand. You still need the .NET 8 SDK to build LoadGen once.

1. **Build the LoadGen bundle** locally:

   ```pwsh
   git clone https://github.com/dbrownems/FabricDaxLoadTest.git
   cd FabricDaxLoadTest
   dotnet publish src/LoadGen/LoadGen.csproj -c Release -r linux-x64 `
     -p:SelfContained=false -p:PublishSingleFile=false -p:UseAppHost=false
   ```

   Output ends up at `src\LoadGen\bin\Release\net8.0\linux-x64\publish\` — about 12 files, ~3.5 MB. (Linux because Fabric Spark hosts are Linux; framework-dependent because the .NET 8 runtime is already on the host. `UseAppHost=false` means no native binary, so no `chmod` step needed after upload.)

2. **In your Fabric workspace** (portal): create a workspace folder named **`LoadTests`** (workspace top bar → **New folder**).

3. **Inside that folder**, create a Lakehouse named **`LoadTests`** (**+ New item → Lakehouse**, schema preview off is fine).

4. **Upload the LoadGen bundle** to `LoadTests.Lakehouse/Files/bin/`. Two easy options:

   - **OneLake File Explorer** (Windows) — sync the workspace, copy the publish output into `LoadTests/LoadTests.Lakehouse/Files/bin/`.
   - **Fabric CLI** — `fab cp -R src/LoadGen/bin/Release/net8.0/linux-x64/publish/ "/<workspace>.workspace/LoadTests.lakehouse/Files/bin/"`.

5. **Seed `queries.json`** at `LoadTests.Lakehouse/Files/queries.json` — start with a one-line corpus and edit later from the `Queries` notebook:

   ```json
   ["EVALUATE ROW(\"x\", 1)"]
   ```

6. **Import the notebooks** from `notebooks/` in this repo (workspace top bar → **Import → Notebook → From this computer**), placing both **inside the `LoadTests` folder**. After import:

   - Rename `Run` → **`LoadTest - Template`** (the import uses the file name).
   - Leave `Queries` named as-is.

You're done. Verify by opening `LoadTest - Template` — cell 2 will detect the template name and refuse to run, prompting Save-As.

---

## Running a load test

The deployed `LoadTest - Template` notebook is **read-only by convention** — every test starts with a Save-As copy.

1. Open `LoadTest - Template` in the workspace.
2. **File → Save As** (or right-click in the workspace → **Duplicate**) and rename the copy to something descriptive — e.g. `LoadTest - DIAD 5u baseline`. Keep it in the `LoadTests` folder.
3. Open the copy. Edit cell **1**:

   ```python
   LOAD_TEST_NAME           = "DIAD 5u baseline"
   LOAD_TEST_DESCRIPTION    = "Baseline 5-user steady run after F4 capacity bump"

   TARGET_WORKSPACE = "MyWorkspace"
   TARGET_DATASET   = "DIAD Final Report with RLS"

   DURATION_SECONDS         = 60
   CONCURRENT_USERS         = 5
   USER_RAMP_TIME_SEC       = 5
   QUERIES_PER_BATCH        = 1
   PAUSE_BETWEEN_ITERATIONS_MS = 500
   PAUSE_BETWEEN_QUERIES_MS    = 0
   TARGET_REPLICA           = ""        # "readonly" → scale-out read replica
   SKIP_RESULTS             = False

   QUERIES_INLINE = []                  # [] → read Files/queries.json
   USERS_INLINE   = []                  # [] → all users share the interactive identity
   ```

4. **Run All**. Cell 4 prints a live status line every second; press **Interrupt Kernel** (■) to cancel — the subprocess receives SIGINT and drains cleanly.
5. Cell **5b** writes the run into the four Delta tables (idempotent — safe to re-run; rows for this `RunId` are deleted and rewritten).
6. Cell **6** plots latency / QPS / users from the per-run CSV.

After the run, the Delta tables are queryable as a Direct Lake source — point a semantic model + Power BI report at them for cross-run analysis.

### Editing the query corpus

Open the `Queries` notebook to read or replace `Files/queries.json`. The runner notebook reads this file by default; setting `QUERIES_INLINE` in cell 1 overrides it for that one run only.

### RLS / impersonation

Each entry in `USERS_INLINE` is `{"email": "...", "role": "..."}`. The email is forwarded to AS as `EffectiveUsername` and the role as `Roles=`. The interactive token holder needs **Build** permission on the model and the right to test as that role; otherwise leave the role empty.

---

## Local CLI

The same `LoadGen` binary that runs in the notebook also runs locally — useful for ad-hoc tests against PBI in your tenant without involving a workspace lakehouse.

```bash
git clone https://github.com/dbrownems/FabricDaxLoadTest.git
cd FabricDaxLoadTest
dotnet build -c Release

dotnet run --project src/LoadGen -c Release -- \
  --xmla    "powerbi://api.powerbi.com/v1.0/myorg/MyWorkspace" \
  --dataset "My Semantic Model" \
  --queries-file samples/queries.json \
  --users-file   samples/users.json \
  --users 50 --duration 120 --ramp-time 30 \
  --token-file   samples/token.txt
```

Acquire a bearer token (audience `https://analysis.windows.net/powerbi/api`) into `samples/token.txt`. From an `az`-logged-in shell:

```pwsh
az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv | Set-Content samples\token.txt
```

### CLI options

| Option | Default | Description |
|---|---|---|
| `--xmla` | *(required)* | XMLA endpoint URL |
| `--dataset` | *(required)* | Semantic model name |
| `--queries-file` | *(required)* | Path to queries.json |
| `--users-file` | *(required)* | Path to users.json (use `[]` to skip impersonation) |
| `--duration` | 60 | Test duration in seconds |
| `--users` | 100 | Concurrent simulated users |
| `--ramp-time` | 30 | Seconds to ramp from 0 → `--users` |
| `--queries-per-batch` | 1 | Concurrent queries per user |
| `--pause-iterations` | 1000 | Pause between iterations (ms) |
| `--pause-queries` | 0 | Pause after each query (ms) |
| `--replica` | `""` | `readonly` to target the scale-out read replica |
| `--skip-results` | false | Drain rows without parsing them client-side |
| `--log-dir` | `./logs` | Directory for telemetry CSV |
| `--token-file` | — | File containing a PBI bearer token |
| `--token` | — | Inline bearer token (avoid; prefer `--token-file`) |

The token must be scoped for `https://analysis.windows.net/powerbi/api`.

---

## Output

### Per-run CSV

One row per query, written to `Files/runs/<RunId>/LoadTest.<users>u.<timestamp>.csv` in the lakehouse (or `--log-dir` for local runs):

```
RunId,UserIndex,UserEmail,QueryIndex,Iteration,StartUtc,EndUtc,
StartTimeMs,DurationMs,Outcome,RowCount,ResponseBytes,ErrorMessage,
ActiveUsersAtStart
```

### Delta tables

Cell 5b in the runner notebook MERGEs run metadata into three small dimensions and bulk-loads the query-execution facts:

| Table | Grain | Notes |
|---|---|---|
| `LoadTests` | one row per logical test (`LoadTestId`) | Carries name + description from cell 1. |
| `LoadTestRuns` | one row per `RunId` | All run-level rollups (`P50/P95/P99/MeanMs`, `Status`, `AbortReason`, `QueryCorpusHash`) plus configuration snapshot. |
| `LoadTestQueries` | one row per `(LoadTestId, QueryIndex)` | The DAX corpus that was used for this test, hashed for change-detection. |
| `LoadTestQueryExecutions` | one row per query execution (the CSV, in Delta form) | Idempotent: existing rows for the run's `RunId` are deleted and rewritten. |

All tables include `OwnerType` / `OwnerId` / `OwnerKey` columns so future trace facts (capture mode, monitor mode) can graft into the same star.

---

## How it scales

Two engineering details worth calling out:

1. **ThreadPool pre-warm.** A 2-core Python notebook host has `MinThreads = 2` by default and grows the worker pool at ~1 thread/sec. With 100 sync-blocking ADOMD.NET drivers, ramp would otherwise serialize to ~100 seconds. `QueryRunner` calls `ThreadPool.SetMinThreads(nUsers + 32, ...)` up front, so workers are eager-allocated and ramp follows the configured `--ramp-time`.

2. **Pre-warm connection.** The first connection to a cold model pays the engine cold-start (50–100 s on a cold capacity). `QueryRunner` opens one warmup connection on the main thread before launching user tasks, so per-user `Open()` times reflect socket cost only — clean numbers for capacity planning.

3. **Out-of-process orchestration.** The notebook launches `dotnet LoadGen.dll` as a subprocess and parses NDJSON envelopes from its stdout. This avoids fighting with sempy over CLR initialization in the notebook host and keeps the kernel responsive (Ctrl+C reliably cancels, status updates render in real time).

---

## Building from source

```pwsh
dotnet build -c Release                                       # build everything
dotnet publish src/LoadGen -c Release -r win-x64              # local LoadGen.exe
dotnet publish src/LoadGen -c Release -r linux-x64 `
  -p:SelfContained=false -p:UseAppHost=false                  # what Deploy-LoadTests.ps1 does
```

Re-running `scripts/Deploy-LoadTests.ps1` will pick up the new bits and refresh `Files/bin/` in the lakehouse.

To regenerate the notebooks from `scripts/build_notebooks.py`:

```pwsh
python scripts/build_notebooks.py
```

Always commit the regenerated `notebooks/*.ipynb` so non-builders can deploy from a fresh clone.

## License

[MIT](LICENSE).
