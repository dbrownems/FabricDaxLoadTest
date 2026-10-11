# Data dictionary

Reference for the six Delta tables that `LoadTest-Main.ipynb` writes to the
lakehouse (when `LAKEHOUSE_NAME` is set in cell 1). Each section lists every
column with a one-line description and links out to deeper material where
useful.

If you're new to the **Analysis Services / Tabular engine** that backs Power
BI semantic models, skim the short primer below first — it explains the
acronyms (FE, SE, VertiPaq, DirectQuery) that recur throughout the
`QueryExecutions` table.

- [Tabular engine primer](#tabular-engine-primer)
- [`LoadTests`](#loadtests) — one row per logical test
- [`LoadTestRuns`](#loadtestruns) — one row per execution
- [`Queries`](#queries) — global query dim
- [`QueryVisuals`](#queryvisuals) — global (query, visual) dim
- [`QueryExecutions`](#queryexecutions) — per-query-attempt facts (the big one)
- [`TraceEvents`](#traceevents) — raw XMLA engine trace rows

The full schema definitions live in [`src/fdlt_runtime/persist.py`](../src/fdlt_runtime/persist.py)
(search for `StructType`).

---

## Tabular engine primer

Every DAX query against a Power BI semantic model runs in
**Analysis Services Tabular**, the engine that hosts the model. Most of the
per-query columns in `QueryExecutions` come straight out of AS's internal
profiling, so the table makes much more sense once you know what the engine
does.

### The two engines inside Tabular

A single DAX query is processed by **two cooperating sub-engines**:

- **Formula Engine (FE)** — the single-threaded "outer brain". Parses the
  DAX, builds the query plan, evaluates iterators and DAX functions that
  can't be pushed down (`SUMX`, `CALCULATE` with complex filters, ranking,
  text manipulation, etc.), and stitches together the results of SE
  scans into the final tabular result. The FE is the part of the engine
  that's expensive to scale: it runs on **one CPU core per query**, so
  FE-heavy queries don't get faster on a bigger capacity SKU. The FE is
  also the source of most "this DAX is slow" optimizations — see SQLBI's
  [Understanding DAX query plans](https://www.sqlbi.com/articles/understanding-dax-query-plans/).
- **Storage Engine (SE)** — the workhorse that reads data. Two flavors
  depending on the model storage mode:
  - **VertiPaq** (Import / Direct Lake) — in-memory columnar engine.
    Massively parallel (one job per segment per CPU), extremely fast.
    Scans return small aggregate tables ("data caches") that the FE
    consumes. This is what's running when you see `VertiPaq*` columns
    populated.
  - **DirectQuery** — pushes the scan down as a SQL query against the
    source (Fabric Warehouse, SQL endpoint, etc.). Wall-clock cost is
    dominated by the source's query latency, not the engine itself. This
    is what's running when you see `DirectQuery*` columns populated.

A useful mental model: **SE returns rectangles, FE turns them into the
answer.** A well-tuned DAX query spends most of its time in SE (cheap,
parallel); a poorly-tuned one spends most of its time in FE (expensive,
serial).

### Walk-through of one query's life

1. Client sends DAX over the XMLA endpoint. LoadGen stamps each query
   with its own `ActivityId` (see [`tracing.md`](tracing.md#correlation));
   AS assigns a `RequestId`.
2. **FE** parses, builds a logical plan, then a physical plan.
3. Plan executes: FE drives, calling **SE** for each scan needed.
   Each SE call is one **VertiPaq scan** or **DirectQuery** push-down.
4. FE assembles SE results, applies remaining DAX, materializes the
   final rowset.
5. AS emits an **`ExecutionMetrics`** trace event summarizing CPU and
   duration totals across FE + SE.
6. Result streamed back to the client.

### Where the load-test columns come from

This tool subscribes to a small set of AS server-side **XMLA trace
events** during each load test, then back-fills metrics from those
events onto the per-execution row:

| Trace event | Populates |
|---|---|
| `ExecutionMetrics` | `RequestId`, `EngineDurationMs`, `EngineCpuMs`, `SECpuMs`, `FECpuMs`, `ExecutionDelayMs`, `CapacityThrottlingMs`, `PeakMemoryKB`, `ExecutionMetricsJson` |
| `DirectQueryEnd` | `DirectQueryCount`, `DirectQueryDurationMs`, `DirectQueryCpuMs` |

`ExecutionMetrics` rows are matched to executions by the `ActivityId`
LoadGen sent with each query (its last 4 bytes encode `QuerySeq`).
`DirectQueryEnd` rows carry only the `RequestId`, so they're matched
through the `ExecutionMetrics` row with the same `RequestId`. See
[`tracing.md`](tracing.md) for the captured events and their limits.

### Wall-clock budget

The columns sum (roughly) like this:

```
ClientDurationMs                                  total client wait
  = EngineDurationMs                              + network / serialization
    = EngineCpuMs                                 serial CPU work (FE + SE, summed across cores)
      + (engine wall - engine CPU - exec delay    DQ wait + parallel-SE wait + throttle
         - DirectQueryDurationMs)                 ^ residual is "unaccounted parallelism"
    + ExecutionDelayMs                            queueing for an engine worker slot
    + CapacityThrottlingMs                        capacity-router throttling (Fabric SKUs)
```

`EngineCpuMs` is **serialized CPU**: if a VertiPaq scan used 8 cores for
100 ms, that's 800 ms of `SECpuMs` even though the wall-clock cost was
only 100 ms. That's why `EngineCpuMs` can — and routinely does —
**exceed `EngineDurationMs`** on import / Direct Lake models. It's not a
bug; it's the metric that lines up with Fabric Capacity Metrics (which
also reports CPU-seconds consumed, not wall-clock).

### Going deeper

- [SQLBI — Understanding the DAX query plan](https://www.sqlbi.com/articles/understanding-dax-query-plans/)
- [SQLBI — Understanding storage engine and formula engine](https://www.sqlbi.com/articles/understanding-storage-engine-and-formula-engine-in-dax/)
- [SQLBI — Capturing DAX queries with DAX Studio](https://www.sqlbi.com/articles/capturing-and-replaying-dax-queries-with-dax-studio/)
- [Microsoft Learn — Analysis Services trace events](https://learn.microsoft.com/en-us/analysis-services/trace-events/analysis-services-trace-events)
- [Microsoft Learn — ExecutionMetrics event class](https://learn.microsoft.com/en-us/analysis-services/trace-events/queries-events-data-columns)

---

## `LoadTests`

One row per logical test — in practice one per notebook. Keyed by
`LoadTestId`, built from the workspace and notebook names. `MERGE`
semantics — re-running the same notebook upserts this row in place.

| Column | Type | Description |
|---|---|---|
| `LoadTestId` | string | `lt-<workspace-slug>-<notebook-slug>` (lower-case, non-alphanumerics → `-`). Stable across runs of the same notebook in the same workspace; changes if either is renamed. Primary key. Joins to `LoadTestRuns.LoadTestId` and `QueryExecutions.LoadTestId`. |
| `Name` | string | The notebook's display name (e.g. `LoadTest-Main`). |
| `WorkspaceId` | string | GUID of the **driver** workspace (the one hosting the notebook). |
| `WorkspaceName` | string | Friendly name of the driver workspace. |
| `NotebookId` | string | GUID of the notebook that ran the test. |
| `NotebookName` | string | Friendly name of the notebook. |
| `TargetWorkspace` | string | Workspace hosting the semantic model under test (may differ from `WorkspaceName`). |
| `TargetDataset` | string | Semantic-model name under test. |
| `SourceType` | string | How the scenario was authored. Currently always `"HandAuthored"`. |
| `QueryCount` | int | Number of distinct queries in the most-recent run's scenario. |
| `ScenarioHash` | string | Hash of the full query list. Two runs with the same `ScenarioHash` executed identical workloads. |
| `LastRunAtUtc` | timestamp | `StartedAtUtc` of the most-recent run. |
| `LastRunId` | string | `RunId` of the most-recent run. |
| `Status` | string | `"Active"` (reserved for future deprecation tagging). |

---

## `LoadTestRuns`

One row per execution. **Append-only** in practice (each run mints a fresh
`RunId`), but written via `MERGE` so a rerun of the persist step on the same
`RunId` is idempotent.

| Column | Type | Description |
|---|---|---|
| `RunId` | string | Primary key. `run-yyyyMMdd-HHmmss` (UTC) minted by LoadGen at run start. |
| `LoadTestId` | string | FK → `LoadTests.LoadTestId`. |
| `RunName` | string | `<LoadTestName>-NN` where NN is the 1-based run sequence within this LoadTest, computed at write time. |
| `OwnerType` | string | Always `"LoadTestRun"` here; the column exists so `TraceCapture`-originated rows in `QueryExecutions` / `TraceEvents` can share the schema (`OwnerType="TraceCapture"`). |
| `OwnerId` | string | Equal to `RunId` for load tests. |
| `OwnerKey` | string | `"LoadTestRun/<RunId>"`. Convenience composite for joins / slicers. |
| `ScenarioHash` | string | Hash of the query list this run executed. Compare across runs to verify workload identity. |
| `StartedAtUtc` | timestamp | When LoadGen started executing. |
| `EndedAtUtc` | timestamp | When LoadGen finished (or aborted). |
| `TargetWorkspace` | string | Workspace name (resolved) of the model under test. |
| `TargetDataset` | string | Semantic-model name. |
| `XmlaEndpoint` | string | The `powerbi://api.powerbi.com/...` URI ADOMD connected to. |
| `Replica` | string | `"readonly"` if the run targeted a scale-out read replica, else empty. |
| `UserCount` | int | Concurrent virtual users (`CONCURRENT_USERS` from cell 1). |
| `DurationSec` | int | Target duration (`DURATION_SECONDS`). |
| `RampSec` | int | Ramp-up window (`USER_RAMP_TIME_SEC`). |
| `ConcurrentQueriesPerUser` | int | In-flight queries per user (1 = serial). |
| `PauseIterMs` | int | Think-time between iterations of the query list (`PAUSE_BETWEEN_ITERATIONS_MS`). |
| `PauseQueryMs` | int | Think-time between queries within an iteration (`PAUSE_BETWEEN_QUERIES_MS`). |
| `SkipResults` | bool | `True` → LoadGen drained rows without materializing them (lighter client load). |
| `TotalQueries` | int | All execution attempts. |
| `SuccessfulQueries` | int | Attempts with `Outcome="Success"`. |
| `FailedQueries` | int | Attempts with `Outcome="Error"`. |
| `Qps` | double | `SuccessfulQueries` ÷ the run's elapsed seconds as measured by LoadGen — trace setup, warm-up and ramp included, so it understates the steady-state rate. `0.0` if LoadGen produced no summary. |
| `Status` | string | `Completed`, or `Cancelled` (the run was interrupted; LoadGen exit code 130). `Aborted` is reserved for runs persisted with an error — the notebook doesn't persist failed runs today. |
| `AbortReason` | string | LoadGen's error message when `Status = Aborted`; NULL otherwise. |
| `P50Ms` | double | 50th percentile of `ClientDurationMs` across this run's successful executions. |
| `P95Ms` | double | 95th percentile. |
| `P99Ms` | double | 99th percentile. |
| `MeanMs` | double | Mean. |
| `RuntimeVersion` | string | `fdlt_runtime` wheel version. |

---

## `Queries`

Global query dim. Keyed by `QueryHash` (SHA-256 of the trimmed DAX text).
**Insert-only** — existing rows are preserved so `FirstSeenAtUtc` stays
stable across re-runs.

| Column | Type | Description |
|---|---|---|
| `QueryHash` | string | Primary key. SHA-256 of the DAX text, trimmed. |
| `QueryShapeHash` | string | Hash of the query **shape** (literals normalized) — lets you group queries that differ only in filter values. |
| `QueryText` | string | The full DAX text. |
| `FirstSeenAtUtc` | timestamp | Timestamp of the first run that referenced this query. |

Joins to `QueryExecutions.QueryHash` and `QueryVisuals.QueryHash` (both M:1).

---

## `QueryVisuals`

Global `(QueryHash, VisualId)` dim populated from the Power BI
**Performance Analyzer** export. Each *Execute DAX Query* event in the
export is preceded by a *Visual Container Lifecycle* event carrying the
visualId, title, and type that issued the query — this table captures that
link so dashboards can break duration / CPU down by visual without
re-parsing the JSON. `MERGE` on `(QueryHash, VisualId)` keeps title / type
fresh across runs.

| Column | Type | Description |
|---|---|---|
| `QueryHash` | string | FK → `Queries.QueryHash`. |
| `VisualId` | string | Power BI visual GUID. |
| `VisualTitle` | string | Visual's display title at capture time. |
| `VisualType` | string | Visual class (e.g. `tableEx`, `barChart`, `card`). |

---

## `QueryExecutions`

The big one. One row per query execution attempt. Generic across the load
tester and any future Trace Capture flow; for load tests `Source="LoadTestRun"`
and `SourceId=<RunId>`.

Each notebook run mints a fresh `RunId`, so in practice rows are **appended**
per run — prior runs are preserved untouched. (Internally the writer
`DELETE`s then `INSERT`s for the current `(Source, SourceId)` so a re-run
of just the persist step on the same `RunId` is idempotent, but that's an
implementation detail.)

### Identity / correlation

| Column | Type | Description |
|---|---|---|
| `Source` | string | `"LoadTestRun"` for this tool. Schema-shared with future `"TraceCapture"` rows. |
| `SourceId` | string | `RunId` for load tests. |
| `LoadTestId` | string | FK → `LoadTests.LoadTestId`. |
| `UserIndex` | int | Virtual-user ordinal within the run (0..`UserCount-1`). |
| `UserEmail` | string | Impersonated user (`EffectiveUserName`) — see [docs/impersonation.md](impersonation.md) — or `cd:<CustomData>` when only `CustomData` is set; NULL without impersonation. |
| `QueryIndex` | int | Position in the scenario's query list. |
| `QueryHash` | string | FK → `Queries.QueryHash`. |
| `Iteration` | int | Pass through the query list this virtual user is on. |
| `QuerySeq` | int | Sequence number of the execution attempt within the run (shared counter across all users; a retry after a reconnect gets a new number). Encoded in the last 4 bytes of the `ActivityId` LoadGen sends with the query. |
| `RequestId` | string | AS-server request ID (back-filled from the `ExecutionMetrics` trace via `(SourceId, QuerySeq)`). Joins to `TraceEvents.RequestId`. |
| `LogicalSessionId` | string | `<LoadTestId>:<UserIndex>` pseudo-id. One logical session per virtual user (the AS-server `SessionId` is not emitted on `ExecutionMetrics`). |

### Time

| Column | Type | Description |
|---|---|---|
| `StartUtc` | timestamp | Query start, UTC. |
| `EndUtc` | timestamp | Query end, UTC. |
| `StartTimeMs` | double | Milliseconds since `LoadTestRuns.StartedAtUtc`. Use this for sub-second relative x-axes. |
| `StartTimeSec` | int | `floor(StartTimeMs / 1000)`. Pre-bucketed because Direct Lake doesn't bin DOUBLE columns gracefully. |
| `StartTime10Sec` | int | `floor(StartTimeMs / 10000) * 10`. 10-second bucket. |
| `StartTime30Sec` | int | `floor(StartTimeMs / 30000) * 30`. 30-second bucket. |

### Latency (wall-clock)

| Column | Type | Description |
|---|---|---|
| `ClientDurationMs` | double | End-to-end wall-clock time measured by the client (ADOMD), including network + serialization. **This is what your users see.** |
| `EngineDurationMs` | long | AS-server-side wall-clock for the query. From `ExecutionMetrics.durationMs`. Excludes client / network. |

### CPU (serialized across cores)

These columns all measure **CPU-seconds consumed**, not wall-clock. On
multi-core scans, CPU can exceed wall-clock. They line up 1:1 with what
**Fabric Capacity Metrics** reports.

| Column | Type | Description |
|---|---|---|
| `EngineCpuMs` | long | Total engine CPU for the query (FE + SE). From `ExecutionMetrics.totalCpuTimeMs`. |
| `FECpuMs` | long | **Formula Engine** CPU. Always single-threaded per query. High FE% usually means DAX that can't be pushed to SE — see [SQLBI's storage-vs-formula-engine article](https://www.sqlbi.com/articles/understanding-storage-engine-and-formula-engine-in-dax/). Derived as `totalCpuTimeMs - vertipaqJobCpuTimeMs` in `ExecutionMetrics`. |
| `SECpuMs` | long | **Storage Engine** CPU. Massively parallel for VertiPaq scans (one CPU per segment). High SE% with low wall-clock is the well-tuned case. From `ExecutionMetrics.vertipaqJobCpuTimeMs`. |

### Queueing & throttling

| Column | Type | Description |
|---|---|---|
| `ExecutionDelayMs` | long | Time the query spent **waiting for resources** before execution — typically a thread (engine worker slot) or a memory grant. Non-zero under heavy concurrency (thread-bound) or memory pressure (grant-bound). From `ExecutionMetrics.executionDelayMs`. |
| `CapacityThrottlingMs` | long | Time the query was held by the Fabric capacity router due to **CU throttling**. Non-zero only when the workspace's capacity is over its smoothed CU budget. From `ExecutionMetrics.capacityThrottlingMs`. |

### Memory

| Column | Type | Description |
|---|---|---|
| `PeakMemoryKB` | long | Approximate peak working set for this query, in KiB. From `ExecutionMetrics.approximatePeakMemConsumptionKB`. |

### Storage Engine breakdown

Only the DirectQuery columns are populated: they're aggregated from
`DirectQueryEnd` events, which appear for DirectQuery and composite
models (and for Direct Lake queries that fall back to DirectQuery). The
VertiPaq columns are reserved and always NULL — the high-volume
`VertiPaqSEQueryEnd` event isn't captured; `SECpuMs` covers VertiPaq CPU.

| Column | Type | Description |
|---|---|---|
| `VertiPaqQueryCount` | int | Reserved; always NULL. |
| `VertiPaqDurationMs` | long | Reserved; always NULL. |
| `DirectQueryCount` | int | Number of DirectQuery push-downs (rowsets the engine asked the source for). |
| `DirectQueryDurationMs` | long | Sum of DQ wall-clock durations. Dominates `EngineDurationMs` for DQ-heavy queries. |
| `DirectQueryCpuMs` | long | CPU AS itself spent processing DQ results (not the SQL source's CPU). |

### Raw passthrough

| Column | Type | Description |
|---|---|---|
| `ExecutionMetricsJson` | string | Full `ExecutionMetrics.TextData` JSON. Use this to recover AS-emitted fields we haven't promoted to columns yet (capacity-router throttle details, future schema additions). |

### Result / outcome

| Column | Type | Description |
|---|---|---|
| `Outcome` | string | `Success` or `Error`. |
| `RowCount` | int | Rows in the result set (0 when `SKIP_RESULTS = True`). |
| `ResponseBytes` | long | Reserved; always NULL (rows written by older versions contain `-1`). Response size isn't measured. |
| `ErrorMessage` | string | ADOMD exception text on `Outcome=Error` (first 500 characters), else NULL. |
| `ActiveUsersAtStart` | int | How many virtual users were running when this query started. Lets you compute "duration as a function of concurrency". |

---

## `TraceEvents`

Raw rows from the server-side XMLA trace subscription. Same
`(Source, SourceId)` keying as `QueryExecutions`. Useful when the
back-filled columns aren't enough and you need to look at individual
trace events (e.g. inspecting `TextData` on `ExecutionMetrics` or
`DirectQueryEnd`).

| Column | Type | Description |
|---|---|---|
| `Source` | string | `"LoadTestRun"`. |
| `SourceId` | string | `RunId`. |
| `LoadTestId` | string | FK → `LoadTests.LoadTestId`. |
| `UtcTimestamp` | timestamp | When AS emitted the event. |
| `EventClass` | string | AS event class name (`ExecutionMetrics`, `DirectQueryEnd`, etc.). See [Microsoft Learn — Analysis Services trace events](https://learn.microsoft.com/en-us/analysis-services/trace-events/analysis-services-trace-events). |
| `DurationMs` | long | The event's `Duration` column — set on `CommandEnd` and `DirectQueryEnd`; `0` on events that don't carry it (including `ExecutionMetrics`, whose duration is `durationMs` in `TextData`). |
| `CpuMs` | long | The event's `CPUTime` column; `0` on events that don't carry it (including `ExecutionMetrics` — see `totalCpuTimeMs` in `TextData`). |
| `ApplicationName` | string | Client `Application Name` connection property: `FabricDaxLoadTest/<RunId>` for the load-test connections. |
| `UserName` | string | AS-reported caller user. |
| `SessionId` | string | AS-server session GUID. Requested on `Command*` and `DirectQuery*` events; not available on `ExecutionMetrics` or `Error`. |
| `RequestId` | string | AS-server request GUID. Joins to `QueryExecutions.RequestId`. |
| `ActivityId` | string | The `ActivityID` LoadGen sent with the query. Last 4 bytes decoded big-endian = `QuerySeq`. |
| `DatabaseName` | string | Semantic-model name on the server. |
| `TextData` | string | Event payload — JSON for `ExecutionMetrics`, the source query for `DirectQueryEnd`, etc. **Truncated to 500 characters** in this version (a known limitation; see [`tracing.md`](tracing.md#known-limitations)). |
