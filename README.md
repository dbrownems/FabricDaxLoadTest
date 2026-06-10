# FabricDaxLoadTest

A load testing tool for Microsoft Fabric and Power BI semantic models. Simulates concurrent users executing DAX queries against the **XMLA endpoint** using ADOMD.NET, then lands per-query telemetry in Delta tables for analysis in Power BI.

Designed to run **inside a Fabric PySpark notebook** — no separate VM, no `dotnet build` required for end users. Import a single `.ipynb`, edit cell 1, and **Run All**. An optional deploy script and lakehouse-backed Delta tables are available when you want cross-run history.

## Quick start

The minimal end-to-end flow, assuming you already have a Power BI semantic model you want to load-test:

1. **Capture a workload** with [**Performance Analyzer**](https://learn.microsoft.com/en-us/power-bi/create-reports/performance-analyzer) — available **in both the Fabric portal and Power BI Desktop** (so no Windows device or local `.pbix` is required). Open the report → *View → Performance Analyzer → Start recording → interact with the report (apply slicers, switch pages, refresh visuals) → Export*. This produces a `.json` file describing the exact DAX queries the report fired.
2. **Import the notebook.** Download [`LoadTest-Main.ipynb`](https://github.com/dbrownems/FabricDaxLoadTest/releases/latest) from the latest GitHub release, then in your Fabric workspace: **+ New item → Import notebook → From this computer**.
3. **Run.** Open the imported `LoadTest - Main` notebook:
   - Upload the Performance Analyzer `.json` onto the notebook's **Resources** panel (left sidebar).
   - In cell 1, set `TARGET_DATASET` to the semantic model you want to hit (or leave `None` if the workspace has exactly one model). Defaults are 25 users for 60 s — see [`docs/loadgen-main.md`](docs/loadgen-main.md) to tune the load shape.
   - **Run All**.

Cell 4 plots latency / QPS / users / engine CPU for this Run, read straight from the per-run CSV on the Spark driver — **no lakehouse required**.

> **Want to capture results across runs?** Set `LAKEHOUSE_NAME` in cell 1 to opt in to writing 4 Delta tables (`LoadTests`, `LoadTestRuns`, `QueryExecutions`, `TraceEvents`) keyed so multiple runs land side-by-side and can be queried as a Direct Lake source for cross-run dashboards. Without it, the forensic artifacts (CSVs, `*.log`, `*.trace.csv`) live only on the driver and disappear at session end. The optional [`scripts/Deploy-LoadTests.ps1`](#setup) provisions a `LoadTests` lakehouse for you and pre-bakes cell 1.

> **One Load Test per workspace is the common case.** Edit `LoadTest - Main` directly. If you later need *additional* Load Tests (e.g. a baseline vs. a what-if scenario), **Save As** in the portal to a new name like `LoadTest - <descriptive name>`.

## Concepts

Three nouns thread through the code, the notebook, and the Delta tables:

- **Load Test** — a *named, reusable test configuration* (e.g. `"Main"` or `"DIAD 5u baseline"`). One notebook = one Load Test. Identity lives in the `LoadTests` Delta table, keyed by `LoadTestId` (a hash of the name). Cell 1's `LOAD_TEST_NAME` (or the notebook filename: `LoadTest - Main` → `Main`) sets it.
- **Run** — *one execution* of a Load Test. Every Run-All of the notebook mints a fresh `RunId` (timestamp-based GUID) and appends a row to `LoadTestRuns`. Re-running is purely additive — prior Runs are preserved untouched, so you can compare a baseline against a regression Run side-by-side.
- **Scenario** — the *DAX workload* a Run executes: the list of queries (+ optional impersonated users for RLS). Provided per-Run via a `.json` attached to the notebook's *Resources* panel (typically a Power BI Desktop **Performance Analyzer** export), or inline in cell 1. `QueryHash`, `QueryShapeHash`, and `QueryText` land inline on each `QueryExecutions` row, and a `ScenarioHash` on the Run summarizes the whole workload — so you can tell at a glance whether two Runs of the same Load Test executed the same queries.

## Components

| Piece | What it is |
|---|---|
| `QueryRunner.dll` | .NET 8 library: orchestrates concurrent users, opens ADOMD.NET connections, runs DAX, writes per-query telemetry CSV. |
| `LoadGen.dll` | Thin .NET CLI wrapper over `QueryRunner`. Run as `dotnet LoadGen.dll …` (Linux Spark host inside the notebook, or anywhere `dotnet` is installed locally). Bundled inside the `fdlt_runtime` wheel as of v0.5.0 — no separate zip download. |
| `fdlt_runtime` (Python wheel) | **The single deploy artifact.** Bundles `LoadGen.dll` + ADOMD assemblies under `fdlt_runtime/loadgen/` and exposes Python orchestration (`bootstrap`, `run`, `persist`, `analyze`). The notebook is a thin shim, so changing the `WHEEL_URL` in cell 2 to a newer release is the entire upgrade story. |
| `notebooks/LoadTest-Main.ipynb` | Deployed as **`LoadTest - Main`**. Drop a queries `.json` onto Resources → edit cell 1 → Run All. |
| `scripts/Deploy-LoadTests.ps1` | One-shot deploy: `dotnet publish` LoadGen, stages binaries into the wheel source tree, builds the wheel, creates the folder + lakehouse, uploads the wheel, patches cell 2's `WHEEL_URL` to the just-uploaded `abfss://` path, deploys (or refreshes) the runner notebook. |

## Status

- ✅ Notebook-driven DAX load tests against any Fabric/PBI semantic model via XMLA.
- ✅ Delta tables (`LoadTests`, `LoadTestRuns`, `QueryExecutions`, `TraceEvents`) written from the notebook for Power BI Direct Lake reporting. The last two are keyed by `(Source, SourceId)` so a future Trace Capture workflow lands rows in the same physical tables (`Source="LoadTestRun"` for these rows, `Source="TraceCapture"` for capture-originated rows).
- ✅ **Coordinated AS-trace capture** — engine `CpuMs` + `DurationMs` back-filled onto every execution row via per-query `ActivityID` correlation.
- ✅ Schema-enabled lakehouse support (auto-detected) + multi-workspace BYO-lakehouse.
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
    │   │   └── fdlt_runtime-<ver>-py3-none-any.whl   ← LoadGen + ADOMD assemblies bundled inside (pip-installed by cell 2)
    │   └── Tables[/dbo]/              ← /dbo/ added when lakehouse is schema-enabled
    │       ├── LoadTests
    │       ├── LoadTestRuns
    │       ├── QueryExecutions      ← keyed (Source, SourceId); QueryHash/QueryShapeHash/QueryText inline
    │       └── TraceEvents          ← keyed (Source, SourceId)
    └── LoadTest - Main (Notebook)     ← edit cell 1 + drop a queries .json on Resources + Run All
```

Everything (lakehouse, notebooks, files) lives inside the `LoadTests` workspace folder. The runner notebook **discovers the destination lakehouse via cell-1 parameters** (`LAKEHOUSE_WORKSPACE_NAME` defaults to the current workspace; `LAKEHOUSE_NAME` defaults to `LoadTests`). No UI lakehouse-attach step is required. Per-run forensic artifacts (raw executions CSV, trace CSV, `result.json`, `*.log`) stay on the Spark driver's local disk under `/tmp/fdlt-<RunId>/` — cell 3 prints the path. Only Delta tables are written to OneLake.

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

The script is idempotent and safe to re-run: every time it `dotnet publish`es LoadGen, builds a fresh `fdlt_runtime` wheel with those binaries embedded, uploads the wheel to `Files/`, and rebakes cell 2's `WHEEL_URL` on the deployed `LoadTest - Main` notebook so it points at the just-uploaded wheel. Cell 1 (your parameters) lives on Save-As copies (`LoadTest - <name>`) which the deploy never touches.

Useful flags:

| Flag | Effect |
|---|---|
| `-SkipPublish` | Skip `dotnet publish`; reuse the existing publish output and just rebuild + re-upload the wheel. |
| `-SkipNotebookUpdate` | Leave an existing `LoadTest - Main` untouched (don't rebake `WHEEL_URL`). Use only if you've made manual edits to cells ≥ 2 in the portal that you don't want clobbered. |

### Option B — Manual setup

For users who can't run the deploy script (no local CLIs, restricted network, no .NET SDK on their machine, etc.). Pre-built artifacts are attached to every [GitHub Release](https://github.com/dbrownems/FabricDaxLoadTest/releases) — no compilation required.

1. **Download `LoadTest-Main.ipynb`** from the [latest release](https://github.com/dbrownems/FabricDaxLoadTest/releases/latest). Cell 2's `WHEEL_URL` is pre-baked to the matching `fdlt_runtime-<ver>-py3-none-any.whl` on the same release, so the notebook is the only file you need.

2. **Import the notebook** into your Fabric workspace (portal → **Import → Notebook → From this computer**).

3. **Point it at any lakehouse you have write access to** — edit cell 1's `LAKEHOUSE_NAME` (and `LAKEHOUSE_WORKSPACE_NAME` if it lives in a different workspace). The Delta tables auto-create on first run; no lakehouse-side prep required.

4. **Upload your queries `.json`** onto the notebook's **Resources** panel (or set `QUERIES_FILE` in cell 1 to an `abfss://…/Files/…` path on the lakehouse).

5. **Run All.** Cell 2 `pip install`s the wheel from the GitHub release on first run; subsequent runs reuse the kernel-installed copy.

> **Network restrictions?** If your Spark driver can't reach `github.com`, download the `.whl` separately, upload it to the lakehouse's `Files/` via the explorer, and change cell 2's `WHEEL_URL` to the `abfss://…/Files/<wheel-filename>` path before Run-All.

> **Updating later.** Edit `WHEEL_URL` in cell 2 to a newer release's wheel URL (e.g. bump `v0.5.0` → `v0.6.0`) and Run All. That's the entire upgrade story — no separate zip download, no notebook re-import.

---

## Running a load test

The deployed `LoadTest - Main` notebook is meant to be **edited and run directly**. The notebook has just four code cells: **(1)** configuration, **(2)** bootstrap, **(3)** run + persist, **(4)** charts.

1. Open `LoadTest - Main` in the workspace. (Or, if you need *additional* Load Tests in the same workspace, **File → Save As** → rename to `LoadTest - <descriptive name>` and keep it in the `LoadTests` folder.)
2. **Set up the Scenario.** Two options:
   - **Drop a queries `.json` onto the notebook's *Resources* panel** (left sidebar). If exactly one `.json` is attached, the notebook picks it up automatically. Power BI Desktop's *Performance Analyzer* exports work verbatim; plain DAX-string lists also work — see [Scenario formats](#scenario-formats).
   - **Or edit `QUERIES_INLINE` in cell 1** with the DAX you want to drive. The notebook ships with a 3-query model-agnostic warm-up Scenario that's only useful for smoke-testing the pipeline.
3. **Edit cell 1.** Cell 1 ships with one-line comments — full reference in [`docs/loadgen-main.md`](docs/loadgen-main.md). The defaults are sensible; you typically only need to override a few:

   ```python
   TARGET_DATASET   = None            # None → the only model in TARGET_WORKSPACE
                                      #   (error if 0 or >1; specify by name otherwise)
   TARGET_WORKSPACE = None            # None → current workspace

   DURATION_SECONDS   = 60
   CONCURRENT_USERS   = 25
   USER_RAMP_TIME_SEC = 15

   LAKEHOUSE_NAME     = None          # None → no persistence; charts read local CSV.
                                      #   Set to a lakehouse name to write 5 Delta
                                      #   tables for cross-run analysis.
   ```

   See [`docs/loadgen-main.md`](docs/loadgen-main.md) for every other parameter (load-shape advanced knobs, RLS users, BYO lakehouse, schema override, log folder, runtime wheel, etc.).
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

If you point the notebook at a BYO lakehouse (by changing `LAKEHOUSE_NAME` in cell 1), make sure cell 2's `WHEEL_URL` points at a wheel that lakehouse can reach (either a GitHub release HTTPS URL, or an `abfss://…/Files/<wheel>.whl` URL on a lakehouse you've manually populated). The deploy script only uploads the wheel to the auto-managed `LoadTests` lakehouse.

### Editing the Scenario

The runner loads queries from one of these sources, in order:

1. `QUERIES_FILE = None` (default) **and** exactly one `*.json` is attached to the notebook's **Resources** panel — that file is auto-discovered.
2. `QUERIES_FILE = "name.json"` — loads `builtin/name.json` from Resources.
3. `QUERIES_FILE = "abfss://…"` — escape hatch for cross-lakehouse references.
4. Otherwise → `QUERIES_INLINE` in cell 1 (the 3-query model-agnostic warm-up the notebook ships with).

Per-Run Scenarios travel with the notebook in Resources, so every Load Test is reproducible without coupling to shared state.

#### Scenario formats

The notebook accepts any of these shapes for `queries.json`:

- **[Performance Analyzer](https://learn.microsoft.com/en-us/power-bi/create-reports/performance-analyzer) export** (canonical) — available in both the Fabric portal and Power BI Desktop:

  ```json
  { "version": "1.1.0",
    "events": [
      { "name": "Query End", "query": "EVALUATE TOPN(100, Sales)" },
      { "name": "Query End", "query": "EVALUATE INFO.MEASURES()" }
    ]
  }
  ```

  Open the report → *View → Performance Analyzer → Start recording → interact with report → Export*. Drop the file straight onto Resources.

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

`USERS_FILE` (Resources panel) or `USERS_INLINE` (cell 1) drives RLS / impersonation. With `USERS_FILE = None` (default) and no inline users, all virtual users share the notebook's interactive token (no impersonation).

```json
[
  "alice@contoso.com",
  { "effectiveUserName": "bob@contoso.com", "roles": ["Sales East"] },
  { "customData": "USA" }
]
```

Each entry can be a string (sets `EffectiveUserName=` only) or an object with any of `effectiveUserName` / `customData` / `roles`. Roles can be a string or an array. See [docs/impersonation.md](docs/impersonation.md) for the full schema, combination semantics, model permissions, and a token-acquisition gotcha when running locally.

`USERS_FILE` is **not** auto-discovered — pass an explicit filename. (Auto-discovery of a single `.json` in Resources always goes to `QUERIES_FILE`.)

---

## Local CLI

The same `LoadGen` binary that runs in the notebook also runs locally — useful for ad-hoc tests against PBI in your tenant without involving a workspace lakehouse. See [docs/loadgen-cli.md](docs/loadgen-cli.md) for the full switch reference.

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
| `--concurrent-queries-per-user` | 1 | In-flight queries per user (rolling drain) |
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

One row per query, written to the Spark driver's local `/tmp/fdlt-<RunId>/LoadTest.<users>u.<timestamp>.csv`. Cell 3 prints the path; the file lives for the kernel's lifetime so you can `sftp`/`!cat`/`%fs` it for forensics. Local-CLI runs write to `--log-dir`.

```
RunId,UserIndex,UserEmail,QueryIndex,Iteration,StartUtc,EndUtc,
StartTimeMs,DurationMs,Outcome,RowCount,ResponseBytes,ErrorMessage,
ActiveUsersAtStart
```

The CSV is the input to the Delta-table write — once `QueryExecutions` is populated, the CSV is no longer needed for analytics.

### Delta tables

The notebook MERGEs Run metadata into three small dimensions and bulk-loads the query-execution facts. Writes happen in parallel via a ThreadPool (one Spark job per table).

| Table | Grain | Notes |
|---|---|---|
| `LoadTests` | one row per Load Test (`LoadTestId = lt-<workspace>-<notebook>`) | Identity + provenance: `Name`, `WorkspaceId`/`WorkspaceName`, `NotebookId`/`NotebookName`, `TargetWorkspace`/`TargetDataset`. |
| `LoadTestRuns` | one row per Run (`RunId = run-yyyyMMdd-HHmmss`) | Run-level rollups (`P50/P95/P99/MeanMs`, `Status`, `AbortReason`, `ScenarioHash`) + config snapshot (`UserCount`, `DurationSec`, …) + target (`TargetWorkspace`, `TargetDataset`, `XmlaEndpoint`). |
| `QueryExecutions` | one row per query execution, keyed `(Source, SourceId, …)` | Generic across data origins. For load-test rows: `Source="LoadTestRun"`, `SourceId=<RunId>`. `QueryHash`, `QueryShapeHash` (literals stripped), and `QueryText` are inline. Idempotent on `(Source, SourceId)`: re-running cell 3 deletes and rewrites just that Run's rows. Trace columns (`EngineDurationMs`, `EngineCpuMs`, `SECpuMs`, `FECpuMs`, …) back-filled via `ActivityID` correlation. |
| `TraceEvents` | one row per AS trace event, keyed `(Source, SourceId, …)` | Raw `QueryEnd` / `ExecutionMetrics` / `VertiPaqSEQuery*` / `DirectQueryEnd` / `ProgressReport*` events for forensic drill-down. Same `(Source, SourceId)` scheme as `QueryExecutions`. Best-effort — empty when tracing fails or is disabled. |

All fact tables share the `(Source, SourceId)` natural key so future
data origins (Trace Capture in Phase 4) graft into the same star
without schema changes.

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

Re-running `scripts/Deploy-LoadTests.ps1` will pick up the new bits, rebuild the wheel, upload it to `Files/`, and rebake cell 2's `WHEEL_URL` on `LoadTest - Main`.

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

The workflow runs `dotnet publish` + `python scripts/build_notebooks.py` on a clean Ubuntu runner, stages the LoadGen binaries into the wheel source tree, builds the `fdlt_runtime` wheel with the binaries embedded, and creates a GitHub Release with the wheel + the regenerated notebook attached. The artifacts are what end-users download under [Option B — Manual setup](#option-b--manual-setup).

## Why another load test tool?

Pure-Python REST-based tools (e.g. the [Fabric Toolbox `FabricLoadTestTool`](https://github.com/microsoft/fabric-toolbox/tree/main/tools/FabricLoadTestTool)) have a few inherent limits this tool was built to lift:

- **Real XMLA wire path.** Each simulated user is a real ADOMD.NET connection — same TCP/TLS handshake, model attach, and session lifetime as Power BI Desktop, Excel, and Tabular Editor. REST tools only exercise the REST gateway path.
- **Real thread parallelism, not notebook fan-out.** N users = N native threads in one .NET process. No `notebookutils.notebooks.runMultiple` per-user spin-up, no GIL, no Spark notebook concurrency cap. A 25-user / 60-second test on a starter pool drove 6,336 queries (≈105 qps from one driver pod) in smoke testing.
- **Coordinated engine-trace capture.** An XMLA trace runs alongside the load test and stamps every command with a per-query `ActivityID`. After the run, `QueryExecutions` has `ClientDurationMs`, `EngineDurationMs`, `EngineCpuMs`, SE/FE CPU split, peak memory, etc. on the same row — no post-hoc log correlation. The raw trace lands in `TraceEvents` for forensics.
- **First-class RLS impersonation.** `EffectiveUsername` / `Roles` per simulated user via a `users.json` on the Resources panel.

## License

[MIT](LICENSE).
