# FabricDaxLoadTest

A load testing tool for Microsoft Fabric and Power BI semantic models. Simulates concurrent users executing DAX queries against the **XMLA endpoint** using ADOMD.NET, then lands per-query telemetry in Delta tables for analysis in Power BI.

Designed to run **inside a Fabric PySpark notebook** — no separate VM, no `dotnet build` required for end users. A deploy script (or a few manual portal steps) drops a `LoadTest - Template` notebook + `LoadTests` lakehouse into your workspace; users **Save As** the template per test and run.

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
| `LoadGen.dll` | Thin .NET CLI wrapper over `QueryRunner`. Run as `dotnet LoadGen.dll …` (Linux Spark host inside the notebook, or anywhere `dotnet` is installed locally). |
| `notebooks/LoadTest-Template.ipynb` | Deployed as **`LoadTest - Template`**. Save-As → drop a queries `.json` onto Resources → edit cell 1 → Run All. |
| `scripts/Deploy-LoadTests.ps1` | One-shot deploy: builds LoadGen, zips it, creates the folder + lakehouse, uploads `loadgen-bin.zip`, deploys the template notebook. |

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
    │   │   ├── loadgen-bin.zip        ← LoadGen + ADOMD assemblies (unzipped to /tmp by cell 2)
    │   │   └── runs/<RunId>/          ← per-run telemetry CSVs
    │   └── Tables[/dbo]/                ← /dbo/ added when lakehouse is schema-enabled
    │       ├── LoadTests
    │       ├── LoadTestRuns
    │       ├── LoadTestQueries
    │       └── LoadTestQueryExecutions
    └── LoadTest - Template (Notebook) ← Save-As to start each run; drop a queries .json onto Resources
```

Everything (lakehouse, notebooks, files) lives inside the `LoadTests` workspace folder. The runner notebook **auto-discovers the workspace's `LoadTests` lakehouse** via the Fabric items API and uses it as the default storage for assemblies, telemetry, and Delta output — no UI lakehouse-attach step is required.

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
- the `LoadTest - Template` notebook
- `Files/loadgen-bin.zip` (the LoadGen + ADOMD assemblies)

Useful flag:

| Flag | Effect |
|---|---|
| `-SkipPublish` | Skip `dotnet publish`; reuse the existing publish output and just re-zip + re-upload. |

### Option B — Manual setup

For users who can't run the deploy script (no local CLIs, restricted network, no .NET SDK on their machine, etc.). Pre-built artifacts are attached to every [GitHub Release](https://github.com/dbrownems/FabricDaxLoadTest/releases) — no compilation required.

1. **Download the latest release assets** from the [Releases page](https://github.com/dbrownems/FabricDaxLoadTest/releases/latest):

   | Asset | Purpose |
   |---|---|
   | `loadgen-bin.zip` | The LoadGen binaries (~3.5 MB, .NET 8, Linux). Upload as-is to the lakehouse — cell 2 of the notebook unzips it on each kernel start. |
   | `LoadTest-Template.ipynb` | The runner template (imports as `LoadTest - Template`). |

2. **In your Fabric workspace** (portal): create a workspace folder named **`LoadTests`** (workspace top bar → **New folder**).

3. **Inside that folder**, create a Lakehouse named **`LoadTests`** (**+ New item → Lakehouse**, schema preview off is fine).

4. **Upload `loadgen-bin.zip`** to `LoadTests.Lakehouse/Files/`:

   - In the lakehouse explorer, right-click **Files → Upload → Upload files** and select `loadgen-bin.zip`. Don't extract — the notebook unzips it on each kernel.

   Alternative: [OneLake File Explorer](https://www.microsoft.com/download/details.aspx?id=105222) (Windows) — sync the workspace and drop the zip into `LoadTests/LoadTests.Lakehouse/Files/`.

5. **Import the notebook** (workspace top bar → **Import → Notebook → From this computer**), placing it **inside the `LoadTests` folder**. After import, rename the imported `LoadTest-Template` to **`LoadTest - Template`** (with spaces around the hyphen — that's what cell 2 of the notebook checks for, and what the Save-As workflow expects).

You're done. Verify by opening `LoadTest - Template` — cell 2 will detect the template name and refuse to run, prompting Save-As.

> **Updating later.** When a new release ships, repeat steps 1, 4, and 5 only — the folder and lakehouse stay put. Saved `LoadTest - <name>` notebooks (and their attached Resources) are untouched.

---

## Running a load test

The deployed `LoadTest - Template` notebook is **read-only by convention** — every test starts with a Save-As copy.

1. Open `LoadTest - Template` in the workspace.
2. **File → Save As** (or right-click in the workspace → **Duplicate**) and rename the copy to something descriptive — e.g. `LoadTest - DIAD 5u baseline`. Keep it in the `LoadTests` folder.
3. **Set up the query corpus.** Two options:
   - **Drop a queries `.json` onto the saved copy's *Resources* panel** (left sidebar in the notebook). If exactly one `.json` is attached, the notebook picks it up automatically. Power BI Desktop's *Performance Analyzer* exports work verbatim; plain DAX-string lists also work — see [Query corpus formats](#query-corpus-formats).
   - **Or edit `QUERIES_INLINE` in cell 1** with the DAX you want to drive. The template ships with a 3-query model-agnostic warm-up corpus that's only useful for smoke-testing the pipeline.
4. Open the copy. Edit cell **1**:

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

   QUERIES_FILE   = None                # None = auto-pick the single .json in Resources;
                                        # otherwise "name.json" or an "abfss://..." URL
   QUERIES_INLINE = [                   # used only when no resource file is found
       "EVALUATE ROW(\"ping\", 1)",
       "EVALUATE INFO.TABLES()",
       "EVALUATE INFO.MEASURES()",
   ]

   USERS_FILE     = None                # None = no impersonation; "users.json" to load
                                        # virtual users from Resources (not auto-discovered)
   USERS_INLINE   = []                  # [] = all users share the interactive identity
   ```

4. **Run All**. Cell 4 prints a live status line every second; press **Interrupt Kernel** (■) to cancel — the subprocess receives SIGINT and drains cleanly.
5. Cell **5b** writes the run into the four Delta tables. Every notebook execution mints a fresh `RunId`, so prior runs are preserved untouched — re-running is purely additive. Re-executing **only cell 5b** (after a completed run) is also safe: it overwrites just that one `RunId`'s fact rows in place.
6. Cell **6** plots latency / QPS / users from the per-run CSV.

After the run, the Delta tables are queryable as a Direct Lake source — point a semantic model + Power BI report at them for cross-run analysis.

### Schema-enabled lakehouses (and BYO lakehouses)

Both flat (`Tables/<TableName>`) and schema-enabled (`Tables/<schema>/<TableName>`) lakehouse layouts are supported. Cell 2 auto-detects via the Fabric `properties.defaultSchema` field — schema-enabled lakehouses write to `Tables/dbo/`, flat lakehouses write to `Tables/`. To override, set `LAKEHOUSE_SCHEMA` in cell 1:

```python
LAKEHOUSE_SCHEMA = None    # auto-detect (default)
LAKEHOUSE_SCHEMA = "dbo"   # force schema-enabled writes to Tables/dbo/
LAKEHOUSE_SCHEMA = ""      # force flat writes to Tables/
LAKEHOUSE_SCHEMA = "loadtests"  # any other schema name works too
```

If you point the notebook at a BYO lakehouse (by renaming `LAKEHOUSE_NAME` in cell 2), make sure that lakehouse contains `Files/loadgen-bin.zip` — the deploy script only writes the zip into the auto-managed `LoadTests` lakehouse.

### Editing the query corpus

The runner loads queries from one of these sources, in order (cell 3):

1. `QUERIES_FILE = None` (default) **and** exactly one `*.json` is attached to the notebook's **Resources** panel — that file is auto-discovered.
2. `QUERIES_FILE = "name.json"` — loads `builtin/name.json` from Resources.
3. `QUERIES_FILE = "abfss://…"` — escape hatch for cross-lakehouse references.
4. Otherwise → `QUERIES_INLINE` in cell 1 (the 3-query model-agnostic warm-up the template ships with).

Per-test corpora travel with the saved `LoadTest - <name>` copy in Resources, so every saved test is reproducible without coupling to shared state.

#### Query corpus formats

The notebook accepts any of these shapes for `queries.json`:

- **Power BI Desktop Performance Analyzer export** (canonical):

  ```json
  { "version": "1.1.0",
    "events": [
      { "name": "Query End", "query": "EVALUATE TOPN(100, Sales)" },
      { "name": "Query End", "query": "EVALUATE INFO.MEASURES()" }
    ]
  }
  ```

  In Power BI Desktop, *View → Performance Analyzer → Start recording → interact with report → Export*. Drop the file straight onto Resources.

- **Object array** (one entry per query):

  ```json
  [
    { "query": "EVALUATE ROW(\"x\", 1)" },
    { "query": "EVALUATE INFO.TABLES()" }
  ]
  ```

- **String array** (when you don't need any per-query metadata):

  ```json
  [ "EVALUATE ROW(\"x\", 1)", "EVALUATE INFO.TABLES()" ]
  ```

#### User list formats

`USERS_FILE` (Resources panel) or `USERS_INLINE` (cell 1) drives RLS / impersonation. With `USERS_FILE = None` (default) and no inline users, all virtual users share the notebook's interactive token (no impersonation). To exercise RLS:

- **Object array** with email + role:

  ```json
  [
    { "email": "alice@contoso.com", "role": "Sales East" },
    { "email": "bob@contoso.com",   "role": "Sales West" }
  ]
  ```

  `email` lands on the AS `EffectiveUserName=` connection property; `role` lands on `Roles=`. The notebook's token holder needs **Build** permission on the model and the right to test as those roles.

- **String array** when you only care about `EffectiveUserName`:

  ```json
  [ "alice@contoso.com", "bob@contoso.com" ]
  ```

`USERS_FILE` is **not** auto-discovered — pass an explicit filename. (Auto-discovery of a single `.json` in Resources always goes to `QUERIES_FILE`.)

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

1. **ThreadPool pre-warm.** Without intervention, the Spark driver's .NET ThreadPool starts at `MinThreads = <core-count>` and grows the worker pool at ~1 thread/sec. With 100 sync-blocking ADOMD.NET drivers, ramp would otherwise serialize to ~100 seconds. `QueryRunner` calls `ThreadPool.SetMinThreads(nUsers + 32, ...)` up front, so workers are eager-allocated and ramp follows the configured `--ramp-time`.

2. **Pre-warm connection.** The first connection to a cold model pays the engine cold-start (50–100 s on a cold capacity). `QueryRunner` opens one warmup connection on the main thread before launching user tasks, so per-user `Open()` times reflect socket cost only — clean numbers for capacity planning.

3. **Out-of-process orchestration.** The notebook launches `dotnet LoadGen.dll` as a subprocess and parses NDJSON envelopes from its stdout. This avoids fighting with sempy over CLR initialization in the Spark driver and keeps the kernel responsive (Ctrl+C reliably cancels, status updates render in real time).

---

## Building from source

```pwsh
dotnet build -c Release                                       # build everything
dotnet publish src/LoadGen -c Release -r linux-x64 `
  -p:SelfContained=false -p:UseAppHost=false                  # what Deploy-LoadTests.ps1 does
```

Re-running `scripts/Deploy-LoadTests.ps1` will pick up the new bits and refresh `Files/loadgen-bin.zip` in the lakehouse.

To regenerate the notebooks from `scripts/build_notebooks.py`:

```pwsh
python scripts/build_notebooks.py
```

Always commit the regenerated `notebooks/*.ipynb` so non-builders can deploy from a fresh clone.

### Cutting a release

Releases are produced by `.github/workflows/release.yml` on any version tag push:

```pwsh
git tag v0.2.0
git push origin v0.2.0
```

The workflow runs `dotnet publish` + `python scripts/build_notebooks.py` on a clean Ubuntu runner, packages `loadgen-bin.zip` + the regenerated notebook, and creates a GitHub Release with auto-generated notes. The artifacts are what end-users download under [Option B — Manual setup](#option-b--manual-setup).

## License

[MIT](LICENSE).
