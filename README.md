# FabricDaxLoadTest

A load testing tool for Microsoft Fabric and Power BI semantic models. Simulates concurrent users executing DAX queries against the **XMLA endpoint** using ADOMD.NET, then lands per-query telemetry in Delta tables for analysis in Power BI.

Designed to run **inside a Fabric PySpark notebook** — no separate VM, no `dotnet build` required for end users. A deploy script (or a few manual portal steps) drops a `LoadTest - Main` notebook + `LoadTests` lakehouse into your workspace; you edit cell 1 of that notebook and **Run All**.

## Quick start

The minimal end-to-end flow, assuming you already have a Power BI semantic model you want to load-test:

1. **Capture a workload.** In Power BI Desktop connected to your model: *View → Performance Analyzer → Start recording → interact with the report (apply slicers, switch pages, refresh visuals) → Export*. This produces a `.json` file describing the exact DAX queries the report fired.
2. **Deploy** (see [Setup](#setup) for prereqs):
   ```pwsh
   pwsh ./scripts/Deploy-LoadTests.ps1 -Workspace "<your-workspace>"
   ```
3. **Run.** In the workspace, open `LoadTests/LoadTest - Main`:
   - Drag the Performance Analyzer `.json` onto the notebook's **Resources** panel (left sidebar).
   - Optionally edit cell 1 (`CONCURRENT_USERS`, `DURATION_SECONDS`, etc. — defaults are 25 users for 60 s).
   - **Run All**.

That's it — the four Delta tables under `LoadTests.Lakehouse/Tables/dbo/` are now ready for cross-Run analysis. Cell 4 plots latency / QPS / users for this Run.

> **One Load Test per workspace is the common case.** Edit `LoadTest - Main` directly. If you later need *additional* Load Tests (e.g. a baseline vs. a what-if scenario), **Save As** in the portal to a new name like `LoadTest - <descriptive name>`. Redeploys never overwrite an existing notebook — runtime behavior ships via the wheel in `Files/loadgen-bin.zip`.

## Concepts

Three nouns thread through the code, the notebook, and the Delta tables:

- **Load Test** — a *named, reusable test configuration* (e.g. `"Main"` or `"DIAD 5u baseline"`). One notebook = one Load Test. Identity lives in the `LoadTests` Delta table, keyed by `LoadTestId` (a hash of the name). Cell 1's `LOAD_TEST_NAME` (or the notebook filename: `LoadTest - Main` → `Main`) sets it.
- **Run** — *one execution* of a Load Test. Every Run-All of the notebook mints a fresh `RunId` (timestamp-based GUID) and appends a row to `LoadTestRuns`. Re-running is purely additive — prior Runs are preserved untouched, so you can compare a baseline against a regression Run side-by-side.
- **Scenario** — the *DAX workload* a Run executes: the list of queries (+ optional impersonated users for RLS). Provided per-Run via a `.json` attached to the notebook's *Resources* panel (typically a Power BI Desktop **Performance Analyzer** export), or inline in cell 1. Each Run snapshots its Scenario into `LoadTestQueries` and stores a `ScenarioHash` on the Run, so you can tell at a glance whether two Runs of the same Load Test executed the same workload.

## Why another load test tool?

The existing pure-Python REST-based tools have meaningful limitations:

- No realistic per-user XMLA connection cost (TCP/TLS handshake, model attach).
- Limited to whatever the REST API path exposes.
- Can't easily simulate Row-Level Security via `EffectiveUsername` / `Roles`.
- Python-.NET interop and process pararallelism overhead saps achievable concurrency.
- **Client-side latency only** — no view into what the engine actually did.
- **Notebook-based fan-out** (`notebookutils.notebooks.runMultiple`) spins up
  a Spark notebook per simulated user. That's coarse-grained
  process-per-user concurrency with multi-second startup cost, capped at the
  workspace's notebook-concurrency limit, and the GIL still serializes each
  user's own work.

This tool drives **ADOMD.NET out-of-process** from a single .NET 8 driver,
so each simulated user gets a real XMLA connection — the same path Power BI
Desktop, Excel, and Tabular Editor use. That makes the test results
meaningful for capacity planning.

### Headline differentiators

- **Coordinated engine-trace capture, joined to every client row.** While
  the load test runs, the driver attaches an XMLA trace to the target model
  and stamps every command with a per-query `ActivityID`. After the run,
  `LoadTestQueryExecutions` has both `ClientDurationMs` *and*
  `EngineDurationMs` + `EngineCpuMs` on the same row — back-filled from the
  matching `QueryEnd` trace event. You can immediately see "client says
  36ms, engine says 9ms, the other 27ms was network and our test
  harness" — the exact split capacity planners need, with zero post-hoc
  log-correlation work. The full raw trace (SE/DQ events,
  `ExecutionMetrics` JSON, ProgressReport, JobGraph, …) also lands in
  `LoadTestTraceEvents` for forensic drill-down.

- **Real thread parallelism, not notebook fan-out.** The .NET driver runs
  N simulated users as N native threads inside one OS process. No Python
  interop overhead per query, no Spark-notebook startup cost per user, no
  GIL — just ADOMD.NET pumping XMLA on dedicated threads. A 25-user test
  on a starter pool ran 6,336 queries in 60 seconds (≈ 105 qps from a
  single Fabric driver pod) in our smoke tests; comparable Python
  notebook-fan-out approaches plateau much earlier.

- **Real XMLA connections.** Same wire path as Power BI Desktop / Excel /
  Tabular Editor. Per-user TCP/TLS handshake, model attach, session
  lifetime — all of it is part of the measurement.

## Components

| Piece | What it is |
|---|---|
| `QueryRunner.dll` | .NET 8 library: orchestrates concurrent users, opens ADOMD.NET connections, runs DAX, writes per-query telemetry CSV. |
| `LoadGen.dll` | Thin .NET CLI wrapper over `QueryRunner`. Run as `dotnet LoadGen.dll …` (Linux Spark host inside the notebook, or anywhere `dotnet` is installed locally). |
| `fdlt_runtime` (Python wheel) | Bundled into `loadgen-bin.zip`. Owns notebook orchestration: bootstrap, run, persist, analyze. The notebook is a thin shim over `fdlt_runtime.notebook.bootstrap()` / `.run()` / `.analyze()` so wheel upgrades take effect on the next Run-All without re-saving the notebook. |
| `notebooks/LoadTest-Main.ipynb` | Deployed as **`LoadTest - Main`**. Drop a queries `.json` onto Resources → edit cell 1 → Run All. |
| `scripts/Deploy-LoadTests.ps1` | One-shot deploy: builds LoadGen, builds the wheel, zips both, creates the folder + lakehouse, uploads `loadgen-bin.zip`, deploys the runner notebook (only if it doesn't already exist). |

## Status

- ✅ Notebook-driven DAX load tests against any Fabric/PBI semantic model via XMLA.
- ✅ Per-run telemetry CSV under `Files/runs/<RunId>/`.
- ✅ Delta tables (`LoadTests`, `LoadTestRuns`, `LoadTestQueries`, `LoadTestQueryExecutions`, `LoadTestTraceEvents`) written from the notebook for Power BI Direct Lake reporting.
- ✅ **Coordinated AS-trace capture** — engine `CpuMs` + `DurationMs` back-filled onto every execution row via per-query `ActivityID` correlation.
- ✅ Schema-enabled lakehouse support (auto-detected) + BYO-lakehouse override.
- 🚧 Monitor mode against an external model + load-test-from-trace extractor — designed in `plan.md`, not yet implemented.

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
    └── LoadTest - Main (Notebook)     ← edit cell 1 + drop a queries .json on Resources + Run All
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

The script is idempotent and safe to re-run: it refreshes the workspace folder, lakehouse, and `Files/loadgen-bin.zip` every time, but **never** overwrites an existing `LoadTest - Main` notebook (or any saved `LoadTest - <name>` copy) so your cell-1 edits are preserved. Runtime behavior changes ship via the wheel inside `loadgen-bin.zip`, which the new bootstrap picks up on the next Run-All.

Useful flags:

| Flag | Effect |
|---|---|
| `-SkipPublish` | Skip `dotnet publish`; reuse the existing publish output and just re-zip + re-upload. |
| `-ForceNotebook` | Overwrite `LoadTest - Main` even if it already exists. Use after rewriting `scripts/build_notebooks.py` (rare — the notebook is a thin shim). |

### Option B — Manual setup

For users who can't run the deploy script (no local CLIs, restricted network, no .NET SDK on their machine, etc.). Pre-built artifacts are attached to every [GitHub Release](https://github.com/dbrownems/FabricDaxLoadTest/releases) — no compilation required.

1. **Download the latest release assets** from the [Releases page](https://github.com/dbrownems/FabricDaxLoadTest/releases/latest):

   | Asset | Purpose |
   |---|---|
   | `loadgen-bin.zip` | The LoadGen binaries + `fdlt_runtime` wheel (~4 MB, .NET 8, Linux). Upload as-is to the lakehouse — cell 2 of the notebook unzips it on each kernel start. |
   | `LoadTest-Main.ipynb` | The runner notebook (imports as `LoadTest - Main`). |

2. **In your Fabric workspace** (portal): create a workspace folder named **`LoadTests`** (workspace top bar → **New folder**).

3. **Inside that folder**, create a Lakehouse named **`LoadTests`** (**+ New item → Lakehouse**; schema-enabled is recommended).

4. **Upload `loadgen-bin.zip`** to `LoadTests.Lakehouse/Files/`:

   - In the lakehouse explorer, right-click **Files → Upload → Upload files** and select `loadgen-bin.zip`. Don't extract — the notebook unzips it on each kernel.

   Alternative: [OneLake File Explorer](https://www.microsoft.com/download/details.aspx?id=105222) (Windows) — sync the workspace and drop the zip into `LoadTests/LoadTests.Lakehouse/Files/`.

5. **Import the notebook** (workspace top bar → **Import → Notebook → From this computer**), placing it **inside the `LoadTests` folder**. After import, confirm its display name is **`LoadTest - Main`** (with spaces around the hyphen).

You're done — jump to [Quick start](#quick-start) step 3 to run it.

> **Updating later.** When a new release ships, repeat step 4 only (re-upload `loadgen-bin.zip`). Your notebook stays put with all your edits — the new behavior ships in the wheel and takes effect on the next Run-All.

---

## Running a load test

The deployed `LoadTest - Main` notebook is meant to be **edited and run directly**. The notebook has just four code cells: **(1)** configuration, **(2)** bootstrap, **(3)** run + persist, **(4)** charts.

1. Open `LoadTest - Main` in the workspace. (Or, if you need *additional* Load Tests in the same workspace, **File → Save As** → rename to `LoadTest - <descriptive name>` and keep it in the `LoadTests` folder.)
2. **Set up the Scenario.** Two options:
   - **Drop a queries `.json` onto the notebook's *Resources* panel** (left sidebar). If exactly one `.json` is attached, the notebook picks it up automatically. Power BI Desktop's *Performance Analyzer* exports work verbatim; plain DAX-string lists also work — see [Scenario formats](#scenario-formats).
   - **Or edit `QUERIES_INLINE` in cell 1** with the DAX you want to drive. The notebook ships with a 3-query model-agnostic warm-up Scenario that's only useful for smoke-testing the pipeline.
3. **Edit cell 1.** Every knob lives in cell 1 with an inline comment. The defaults are sensible — you typically only need to override a few:

   ```python
   LOAD_TEST_NAME   = None            # None → derived from notebook name
                                      #   "LoadTest - Foo" → "Foo"
   TARGET_WORKSPACE = None            # None → current workspace
   TARGET_DATASET   = None            # None → the only model in TARGET_WORKSPACE
                                      #   (error if 0 or >1; specify by name otherwise)

   DURATION_SECONDS   = 60
   CONCURRENT_USERS   = 25
   USER_RAMP_TIME_SEC = 15
   ```

   See cell 1 in the notebook for the full list (load-shape knobs, RLS users, BYO lakehouse, schema override, etc.).
4. **Run All.** Cell 3 prints a live status line every second while LoadGen runs; press **Interrupt Kernel** (■) to cancel — the subprocess receives SIGINT and drains cleanly. When the run completes, cell 3 writes the Run into the four Delta tables. Every Run-All mints a fresh `RunId`, so prior Runs are preserved untouched. Re-executing **only cell 3** (after a completed run) is also safe — it deletes and rewrites just that one `RunId`'s fact rows.
5. **Cell 4** plots latency / QPS / users for the Run that just completed, straight from the per-run CSV.

After the Run, the Delta tables are queryable as a Direct Lake source — point a semantic model + Power BI report at them for cross-Run analysis.

### Schema-enabled lakehouses (and BYO lakehouses)

Both flat (`Tables/<TableName>`) and schema-enabled (`Tables/<schema>/<TableName>`) lakehouse layouts are supported. Cell 2 auto-detects via the Fabric `properties.defaultSchema` field — schema-enabled lakehouses write to `Tables/dbo/`, flat lakehouses write to `Tables/`. To override, set `LAKEHOUSE_SCHEMA` in cell 1:

```python
LAKEHOUSE_SCHEMA = None    # auto-detect (default)
LAKEHOUSE_SCHEMA = "dbo"   # force schema-enabled writes to Tables/dbo/
LAKEHOUSE_SCHEMA = ""      # force flat writes to Tables/
LAKEHOUSE_SCHEMA = "loadtests"  # any other schema name works too
```

If you point the notebook at a BYO lakehouse (by changing `LAKEHOUSE_NAME` in cell 1), make sure that lakehouse contains `Files/loadgen-bin.zip` — the deploy script only writes the zip into the auto-managed `LoadTests` lakehouse.

### Editing the Scenario

The runner loads queries from one of these sources, in order:

1. `QUERIES_FILE = None` (default) **and** exactly one `*.json` is attached to the notebook's **Resources** panel — that file is auto-discovered.
2. `QUERIES_FILE = "name.json"` — loads `builtin/name.json` from Resources.
3. `QUERIES_FILE = "abfss://…"` — escape hatch for cross-lakehouse references.
4. Otherwise → `QUERIES_INLINE` in cell 1 (the 3-query model-agnostic warm-up the notebook ships with).

Per-Run Scenarios travel with the notebook in Resources, so every Load Test is reproducible without coupling to shared state.

#### Scenario formats

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

The notebook MERGEs Run metadata into three small dimensions and bulk-loads the query-execution facts. Writes happen in parallel via a ThreadPool (one Spark job per table).

| Table | Grain | Notes |
|---|---|---|
| `LoadTests` | one row per Load Test (`LoadTestId`) | Carries name + description from cell 1. |
| `LoadTestRuns` | one row per Run (`RunId`) | All run-level rollups (`P50/P95/P99/MeanMs`, `Status`, `AbortReason`, `ScenarioHash`) plus configuration snapshot. |
| `LoadTestQueries` | one row per `(LoadTestId, RunId, QueryHash)` | The Scenario (DAX queries) snapshot for this Run, hashed for change-detection. |
| `LoadTestQueryExecutions` | one row per query execution (the per-Run CSV, in Delta form) | Idempotent on `RunId`: re-running cell 3 deletes and rewrites just that Run's rows. |

All tables include `OwnerType` / `OwnerId` / `OwnerKey` columns so future trace facts (capture mode, monitor mode) can graft into the same star.

---

## How it scales

Three engineering details worth calling out:

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
