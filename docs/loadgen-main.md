# `LoadTest - Main` — parameters reference

The `LoadTest - Main` notebook (built from
[`scripts/build_notebooks.py`](../scripts/build_notebooks.py)) exposes every
load-test knob as a top-level variable in **cell 1**. Cell 1 itself is
deliberately terse — most parameters get a one-line comment and the full
explanation lives here.

For a typical run you only need to set a few:

- `TARGET_DATASET` — which semantic model to hit
- `LAKEHOUSE_NAME` — *optional*; opt in to persisting the run as Delta tables
- `DURATION_SECONDS`, `CONCURRENT_USERS`, `USER_RAMP_TIME_SEC` — the load shape

Everything else has a sensible default. Skim the rest of this doc when you
need to override one of them.

---

## Target semantic model

```python
TARGET_WORKSPACE = None   # None → current workspace; "Name"/GUID → cross-workspace
TARGET_DATASET   = None   # None → only model in workspace (errors on 0 or >1)
TARGET_REPLICA   = ""     # "" → primary; "readonly" → scale-out read replica
```

- `TARGET_WORKSPACE = None` resolves to the workspace this notebook lives in
  (via `notebookutils.runtime.context`). Set a name or GUID to drive a model
  in another workspace via XMLA. Cross-tenant guest workspaces are **not**
  supported — `getToken("pbi")` is home-tenant scoped.
- `TARGET_DATASET = None` auto-picks the only semantic model in the target
  workspace. Errors out if there are 0 or more than 1.
- `TARGET_REPLICA = "readonly"` routes via the scale-out read replica
  endpoint (Premium / Fabric capacities with read-replicas enabled). Useful
  for measuring read-only throughput without affecting the writable replica.

## Lakehouse (optional — for persisting runs)

```python
LAKEHOUSE_NAME           = None   # None → no persistence (charts read local CSV)
LAKEHOUSE_WORKSPACE_NAME = None   # None → current workspace
LAKEHOUSE_SCHEMA         = None   # None → auto-detect (schema-enabled → "dbo")
```

Charts in cell 4 read the LoadGen CSV directly from the Spark driver's
local `/tmp/` — they need **no Spark and no lakehouse**.

Set `LAKEHOUSE_NAME` to opt in to writing 6 Delta tables — `LoadTests`,
`LoadTestRuns`, `Queries`, `QueryVisuals`, `QueryExecutions`, `TraceEvents` — keyed
so multiple runs land side-by-side and can be queried as a Direct Lake
source for cross-run dashboards. Without it, the forensic artifacts (CSVs,
`*.log`, `*.trace.csv`) live only on the driver and disappear at session
end.

- `LAKEHOUSE_WORKSPACE_NAME` defaults to the current workspace. Set a name
  or GUID for a BYO-lakehouse in another workspace in your home tenant.
- `LAKEHOUSE_SCHEMA` defaults to auto-detect via the Fabric API:
  schema-enabled lakehouses get `"dbo"`, flat lakehouses get `""`. Override
  with `"dbo"` / `"loadtests"` / etc. to force a specific schema, or `""`
  to force flat layout.

## Load test identity

The Load Test name is **the notebook name with any `LoadTest -` prefix
stripped** (so a notebook named `LoadTest - DIAD` becomes Load Test
`DIAD`). This is the natural key in the `LoadTests` Delta dimension —
to create a separate Load Test, **Save As** the notebook to a new name.
There is no in-cell override.

The `LoadTestId` is `lt-<workspace>-<notebook>` (slugified); the
`RunId` for each execution is `run-yyyyMMdd-HHmmss`, generated at
Run-All time.

## Load shape

```python
DURATION_SECONDS             = 60     # how long virtual users execute queries
CONCURRENT_USERS             = 25     # max concurrency at steady state
USER_RAMP_TIME_SEC           = 15     # linear ramp from 0 → CONCURRENT_USERS
CONCURRENT_QUERIES_PER_USER  = 1      # in-flight queries per user
PAUSE_BETWEEN_ITERATIONS_MS  = 10000  # think-time between iterations per user (~10s ≈ human dwell)
PAUSE_BETWEEN_QUERIES_MS     = 0      # think-time between queries inside an iteration
```

- `DURATION_SECONDS` is steady-state duration **after** the ramp completes.
  Total wall time ≈ `USER_RAMP_TIME_SEC + DURATION_SECONDS`.
- `USER_RAMP_TIME_SEC = 0` adds all users at once. The ramp is linear.
- `CONCURRENT_QUERIES_PER_USER` controls how many ADOMD connections each
  virtual user opens. Each user rolls through the iteration's queries:
  when one finishes, the next pending query is dispatched on the freed
  connection (Power BI Desktop-style — *not* batched all-finish-then-fire).
  `1` is strictly serial.
- `PAUSE_BETWEEN_ITERATIONS_MS` is think-time between full passes over the
  scenario's query list. `PAUSE_BETWEEN_QUERIES_MS` is think-time between
  queries inside one iteration.

## Tracing & result handling

```python
ENABLE_TRACING = True    # capture engine events to TraceEvents table
SKIP_RESULTS   = False   # True → drain rows without parsing
```

- `ENABLE_TRACING = True` subscribes to a server-scoped XMLA trace and
  captures `QueryEnd`, `ExecutionMetrics`, and DirectQuery events into the
  `TraceEvents` table (and the per-run `*.trace.csv`). Requires Build/Read
  on the dataset. Set `False` to skip tracing entirely if you suspect
  trace overhead is affecting numbers, or if you don't have the necessary
  permissions.
- `SKIP_RESULTS = True` drains result rows from ADOMD without parsing them.
  Useful when you want to stress-test the engine and result-set
  marshalling would otherwise dominate. Off by default — the default
  measures the same thing Power BI Desktop measures.

## Scenario (queries to drive)

```python
QUERIES_FILE   = None             # auto-pick single .json in Resources
QUERIES_INLINE = ["EVALUATE …", …]  # fallback if no file resolves
```

The runner resolves the scenario in this order:

1. `QUERIES_FILE = None` **and** exactly one `*.json` is attached to the
   notebook's *Resources* panel — that file is auto-discovered.
2. `QUERIES_FILE = "name.json"` — load `builtin/name.json` from Resources.
3. `QUERIES_FILE = "abfss://…"` — cross-lakehouse / cross-workspace
   escape hatch.
4. Nothing matches → fall back to `QUERIES_INLINE`.

Accepted JSON shapes (full list in [`README.md` § Scenario formats](../README.md)):

- [Performance Analyzer](https://learn.microsoft.com/en-us/power-bi/create-reports/performance-analyzer) export with `events[]`
- `[{"query": "EVALUATE …"}, …]`
- `["EVALUATE …", …]`

The default `QUERIES_INLINE` is a 3-query model-agnostic warm-up — only
useful for smoke-testing the pipeline. Replace with real DAX, or attach a
Performance Analyzer export to the Resources panel, before drawing any
conclusions.

## Virtual users (RLS / impersonation)

```python
USERS_FILE   = None
USERS_INLINE = []   # empty ⇒ all virtual users share the notebook token
```

Resolution order is the same as `QUERIES_FILE`, except auto-discovery does
**not** pick up a stray `.json` for users — single-`.json` Resources always
go to `QUERIES_FILE`. Users must be named explicitly via `USERS_FILE`.

Three entry shapes (see [`docs/impersonation.md`](impersonation.md) for the
full schema and EffectiveUserName / CustomData / Roles semantics):

- `"alice@contoso.com"` → `EffectiveUserName`
- `{"effectiveUserName":"a","customData":"…","roles":["R1"]}`
- `{"customData":"USA"}` → `CUSTOMDATA()` only

## Log folder (raw forensic files)

```python
LOG_FOLDER = None
```

Controls the destination of the LoadGen subprocess's raw artifacts
(executions CSV, engine trace CSV, `*.log`). The Delta tables (when
`LAKEHOUSE_NAME` is set) are written regardless of this setting — this
only affects the raw forensic files.

| Value | Behavior |
|---|---|
| `None` | `/tmp/fdlt-run-<id>/` on the Spark driver. Fast (local SSD) but discarded when the kernel cycles. After the run, `*.log` and `*.trace.csv` are copied to `{LAKEHOUSE}/Files/run-logs/<RunId>/` so they survive (only when a lakehouse is set). |
| `"/lakehouse/default/Files/<folder>"` | LoadGen writes directly there — files are visible in OneLake LIVE as the run progresses. No post-run copy. Use this when you want to tail logs in real time or when the run might be killed before completion. |
| `"abfss://…"` | LoadGen still writes to `/tmp` (the .NET process can't target OneLake directly), but the post-run copy lands under `<LOG_FOLDER>/<RunId>/` instead of the default. Use this to redirect to a different lakehouse / folder for long-term retention or shared review. |

## Runtime wheel (advanced)

```python
WHEEL_URL = "https://github.com/dbrownems/FabricDaxLoadTest/releases/download/vX.Y.Z/fdlt_runtime-X.Y.Z-py3-none-any.whl"
```

`WHEEL_URL` points at the `fdlt_runtime` wheel that cell 2 pip-installs.
The .NET LoadGen binaries ship inside the wheel — there's nothing else to
refresh on upgrade.

Forms supported:

- `https://github.com/dbrownems/FabricDaxLoadTest/releases/download/vX.Y.Z/fdlt_runtime-X.Y.Z-py3-none-any.whl`
  — direct from GitHub; needs outbound internet from Spark.
- `https://github.com/dbrownems/FabricDaxLoadTest/releases/download/v*.*.*/fdlt_runtime-*.*.*-py3-none-any.whl`
  — wildcard form. `*.*.*` is resolved at run time to the latest GitHub
  release tag (one unauthenticated GET against the public releases API).
  Use this to opt INTO auto-upgrade on every Run-All; pin a specific
  version to opt out.
- `abfss://<wsid>@onelake.dfs.fabric.microsoft.com/<lhid>/Files/<file>.whl`
  — offline-friendly; what `scripts/Deploy-LoadTests.ps1` patches in.
- `/lakehouse/default/Files/<file>.whl` — already-attached lakehouse,
  manual upload.

To upgrade: bump the version in the URL (e.g. `v0.9.0` → `v0.10.0`) and
Run-All.

---

## Reading the charts (cell 4)

When cell 3 finishes, cell 4 renders a stacked figure with three or four
panels (the fourth, engine CPU, only appears when the trace produced
`ExecutionMetrics` events — i.e. `ENABLE_TRACING = True` and you have
permission to subscribe to the trace).

For the methodology and what to do with these numbers, see
[`load-testing-overview.md`](load-testing-overview.md). This section is
the quick reference.

### Panel 1 — Query duration

![Query duration panel](img/chart-latency.png)

A blue band (per-bucket min–max) with a mean line and a max line, in
milliseconds. This is **client-side** wall-clock duration as observed
by the simulated user (ADOMD round-trip from `Open()` of the reader to
draining the result rows). Includes serialization, network, and any
queueing.

The engine-side duration (DAX engine only, no client-side cost) is
written per-execution to `QueryExecutions.EngineDurationMs` from the
trace correlation. To compare the two, query the Delta tables — the
chart only shows the client-side number to keep it concise.

### Panel 2 — Throughput

![Throughput panel](img/chart-throughput.png)

Bucketed queries-per-second across the run. If any queries errored,
error QPS is **stacked in red** on top of the success bars. Steady-state
QPS is the test's headline throughput number.

### Panel 3 — Active users

![Active users panel](img/chart-active-users.png)

The number of virtual users actually running at each point in time.
This is the ramp curve as actually executed (vs. as configured).
Useful as a sanity check: the slope should match
`USER_RAMP_TIME_SEC` and the plateau should match `CONCURRENT_USERS`.

### Panel 4 — Engine CPU (when tracing is on)

![Engine CPU panel](img/chart-engine-cpu.png)

Purple bars showing **CPU-seconds per second of wall-clock**, sourced
from `ExecutionMetrics` `totalCpuTimeMs` distributed across each query's
wall-clock interval. The legend reports peak / average / bucket width.

The unit is the **effective number of CPU cores the AS engine kept busy
on average** during each bucket. A peak of 5 means the engine was
running on average 5 cores' worth of CPU during that bucket.

If the capacity throttled any queries (queries waited behind the
smoothing window before the engine accepted them), an **orange line on
a secondary axis** appears showing throttle-seconds-per-second. Any
orange means at least some queries queued; the legend reports throttle
peak and total queued time.

> Engine CPU drives capacity unit (CU) consumption, but the conversion
> factor and smoothing window are capacity- and SKU-specific and aren't
> modeled in this chart. To get the actual CU cost of the Run, use the
> **Capacity Metrics App** — see [Translating engine CPU into capacity
> impact](load-testing-overview.md#translating-engine-cpu-into-capacity-impact).
