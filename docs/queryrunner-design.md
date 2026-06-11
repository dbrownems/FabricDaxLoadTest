# QueryRunner — design overview

This is a navigation aid for contributors changing the .NET load-driver
library at `src/QueryRunner/`. It describes the code as it exists today
(not a target). Read it before you edit `QueryRunner.cs` so you can
jump to the right region of the file instead of scrolling 1400 lines.

> Consumer-facing docs live in [`docs/load-testing-overview.md`](load-testing-overview.md)
> and [`docs/loadgen-main.md`](loadgen-main.md). For the wire trace
> subscription, see comments in `src/QueryRunner/Tracing/TraceSubscriber.cs`.

## What QueryRunner is

A .NET 8 class library that drives a synthetic DAX workload against a
Microsoft Fabric / Power BI XMLA endpoint via ADOMD.NET, captures
per-iteration timings to a CSV, optionally subscribes to a server-side
XMLA trace for engine CPU/duration, and returns a JSON summary. It is
consumed by:

- `src/LoadGen/` — a thin CLI front-end (this repo).
- `python/fdlt_runtime/` — a `pythonnet` wrapper packaged as a Python
  wheel for Fabric notebook use.
- (planned) `PbiLoadTester` from the public Fabric toolbox.

The public surface is intentionally small: one entry point
(`QueryRunner.StartLoadTest`) returning a `LoadTestHandle` that the
caller polls and joins.

## File map

```
src/QueryRunner/
├── QueryRunner.cs        ── 1412 lines  ── single big file (see "Sections" below)
├── LoadTestApi.cs        ──  225 lines  ── public DTOs + LoadTestHandle
├── QueryRunner.csproj    ──   48 lines  ── net8.0, nullable enabled, ADOMD.NET ref
└── Tracing/
    ├── TraceModels.cs        ── 103 lines  ── AS rowset → CSV-shaped events
    └── TraceSubscriber.cs    ── 649 lines  ── XMLA trace subscription
```

`QueryRunner.cs` and `Tracing/TraceSubscriber.cs` are both on the large
side. `QueryRunner.cs` is the next refactoring candidate; the line
ranges below double as a "where to split if/when we split" map.

## Public types (LoadTestApi.cs)

| Type | Lines | Purpose |
|---|---|---|
| `LoadTestConfig` | 23-79 | Input record. Endpoint, dataset, token, queries, slot arrays, durations, error policy, `EnableTracing`. |
| `ErrorPolicy` | 81-86 | `Continue` (default) or `Abort` per-iteration query failure. |
| `LoadTestProgressSnapshot` | 92-145 | Read-only counters + sliding-window stats. Single value the caller polls. |
| `SnapshotBox` | 151-159 | `internal`. Volatile reference cell holding the latest snapshot. |
| `LoadTestHandle` | 173-244 | Returned by `StartLoadTest`. `IsCompleted`, `LatestSnapshot`, `Cancel`, `Wait`, `WaitOrThrow`, `Dispose`. |

These five types, plus `QueryRunner.StartLoadTest`, are the entire
contract `LoadGen` and `fdlt_runtime` rely on. Anything else in the
project should be considered an implementation detail even if its
declared visibility is `public`.

## QueryRunner.cs — sections

`QueryRunner.cs` mixes several concerns in one namespace-level file.
Use these line ranges as bookmarks:

| Lines | Section | Notes |
|---|---|---|
| 1-15 | usings + `namespace FabricDaxLoadTest` | |
| **17-121** | `public sealed class QueryRunnerLogger : IDisposable` | Lock-free file logger. Background consumer thread on a `BlockingCollection<string>`. Caller wires `OnLogLine` for stdout/stderr echo (left null in Livy/Jupyter contexts to avoid corrupting JSON-RPC framing). |
| **123-137** | `public class QueryResult` | Per-iteration outcome (latency, status, query hash, response bytes, active-user snapshot). Used by status counters and the CSV writer. |
| **139-156** | `internal class TelemetryRecord` | What gets serialized into the executions CSV (one row per query attempt, including retries on reconnect). |
| **163-343** | `public class QueryRunnerStatus` (singleton) | Mutable in-memory counters: cumulative + per-window totals, active-user count, in-flight count, P50/P95/P99 reservoir. `RecordQuery` is called from every user task; `SnapshotAndReset` is called by the periodic reporter. Process-wide singleton (`QueryRunnerStatus.Instance`) — safe because of the run gate (one run per process). |
| 331-342 | nested `WindowSnapshot` | Per-window summary returned by `SnapshotAndReset`. |
| **345-1462** | `public static class QueryRunner` | The engine. Sub-sections below. |

### Inside `static class QueryRunner` (345-1462)

| Lines | Sub-section | What it does |
|---|---|---|
| 350-362 | static state | `_logger`, `_activeRun` gate, `_querySeq`. |
| 373-386 | `MakeActivityId(runId, seq)` | Encodes `(runId, seq)` into a deterministic Guid we send as ADOMD `ActivityID`. Lets `persist.py` JOIN executions to `ExecutionMetrics` trace events for engine-CPU back-fill. |
| 397-433 | `StartLoadTest(LoadTestConfig)` | Public entry point. Validates, claims the run gate, allocates `SnapshotBox`, spawns `RunLoadTestCore` on a `Task`, returns `LoadTestHandle`. Synchronous-throws on invalid config or concurrent run. |
| 435-463 | `ValidateConfig` | Argument-shape checks (queries non-empty, slot arrays consistent, endpoint/dataset present, durations positive). |
| **465-1045** | `RunLoadTestCore` | The actual run. ~580 lines. |
| 1047-1144 | `LogWriterLoop` | Background drain of the executions queue into a buffered `StreamWriter` over a `FileStream` opened with `FileShare.Read` so external tailers work mid-run. |
| 1146-1159 | `SanitizeCsvField` | CSV-escapes / truncates one field. |
| 1161-1194 | `SimulateUserWithConnections` | Per-user driver loop: iteration → `RunIteration` → think-time pause. Holds the user's connection array for the lifetime of the run. |
| 1196-1290 | `RunIteration` | Per-iteration query fan-out. `SemaphoreSlim` gate sized to `concurrentQueriesPerUser`, slot-index queue tracks which connection a task uses, transparent reconnect on "connection lost" with a single retry. |
| 1292-1318 | `SubmitTelemetry` | `QueryResult` → `TelemetryRecord` → enqueue into the CSV writer queue. |
| 1320-1401 | `ExecuteQuery` | The hot path: open `AdomdCommand`, set `ActivityID`, run the query, drain rows (or skip with `--skip-results`), build `QueryResult`. ADOMD calls are synchronous. |
| 1403-1427 | `BuildConnectionString` | Assembles the ADOMD connection string (token, EUN, CustomData, Roles, ApplicationName=run-id for trace filtering). |
| 1429-1462 | Slot-array helpers | `SlotCount`, `ThrowIfMismatched`, `NormalizeSlotArray`, `UserLabel`. Normalize the three impersonation arrays to a common length. |
| 1464-end | `BuildStats` + redaction helpers | Final JSON summary; redacts the bearer token from any captured exception text. |

### What `RunLoadTestCore` does, in order (465-1045)

1. **Setup** (469-557): destructure config, normalize slot arrays, init the
   linked `CancellationTokenSource` (caller cancel ⨯ duration timer),
   create `_logger`, seed the initial snapshot, open the executions CSV +
   start `LogWriterLoop`.
2. **Trace subscription** (568-661): if `EnableTracing && logDirectory`,
   create a `TraceSubscriber` (filters server-side trace rows by
   `ApplicationName=FabricDaxLoadTest/<runId>`), open the trace CSV, and
   start a writer task draining `subscriber.Events` into the file.
   Failures here are warnings, not fatal — except the `OnFatalError`
   callback, which sets `traceFatalError` and cancels the run.
3. **Threadpool warmup** (672-685): `ThreadPool.SetMinThreads` sized for
   `users × concurrentQueriesPerUser` so the .NET injection rate does
   not serialize ramp-up on small Fabric notebook hosts.
4. **Pre-warm connection** (706-717): a single up-front `Open()` against
   slot 0 to absorb the gateway/model cold-start (50-100s on a cold
   capacity) before per-user opens hit the front-end.
5. **Ramp** (719-849): one task per user, scheduled with `rampIntervalMs`
   delay. Each task opens its `concurrentQueriesPerUser` connections,
   bumps `connectedUsers` + `status.IncrementActiveUsers`, then jumps
   into `SimulateUserWithConnections`. The main thread loops printing
   ramp progress every `nUsers/10` connections.
6. **Snapshot publisher** (782-822): 1 Hz background task that reads
   `QueryRunnerStatus`, computes a 5 s rolling QPS and the latency
   percentiles, and writes the result into `SnapshotBox` so polling
   callers (LoadGen's chart, `fdlt_runtime`) see live progress.
7. **Steady-state** (909-934): `Task.WaitAll(userTasks)`. The 60 s
   periodic reporter runs in parallel.
8. **Drain & shutdown** (943-1042): cancel the snapshot publisher, wait
   for the periodic reporter, complete the executions queue and join
   the writer, give the trace 5 s to flush in-flight `ExecutionMetrics`
   events, dispose the `TraceSubscriber`, resolve the final phase
   (`Failed > Cancelled > Done`), publish the final snapshot, dispose
   the logger.
9. **Return** (1044): the JSON string built by `BuildStats`. The
   `LoadTestHandle.Wait()` caller receives this.

## Tracing/

`TraceModels.cs` (103 lines) defines `TraceEventRow` — the rowset shape
the trace subscriber emits — and helper methods for converting the
ADOMD rowset row to a CSV-friendly record. It depends only on the
ADOMD client package, not AMO, so the `fdlt_runtime` payload stays
small.

`TraceSubscriber.cs` (649 lines) wires up a server-side
`AS_Server_Trace` via the AMO API (`Microsoft.AnalysisServices`),
filters server-side where possible, filters client-side by
`ApplicationName` (PBI Service rejects most server-side filters),
batches rows through a `Channel<TraceEventRow>`, and surfaces them via
`Events.ReadAllAsync()`. `OnFatalError` is invoked when the trace
reader fails non-recoverably mid-run; the run aborts cleanly. Drain
semantics live in `DisposeAsync`.

## Lifetime, threading, cancellation

```
Caller
  └─► QueryRunner.StartLoadTest         (synchronous; throws on validate failure or concurrent run)
        └─► Task.Run( RunLoadTestCore ) (background task; returned via LoadTestHandle)
              ├── 1× LogWriter task
              ├── 1× Snapshot publisher task   (1 Hz)
              ├── 1× Periodic reporter task    (60 s)
              ├── 0..1× Trace writer task      (when EnableTracing)
              └── nUsers× user driver tasks
                    └── per iteration: SemaphoreSlim-gated query fan-out × concurrentQueriesPerUser
```

- **Cancellation:** `LoadTestHandle.Cancel()` → external `CancellationTokenSource`,
  linked with the duration `CancellationTokenSource`, threaded into ramp
  delays, per-iteration delays, and the snapshot/reporter loops. ADOMD
  calls are synchronous and not cancellable mid-call; cancellation
  takes effect at the next iteration boundary.
- **Async surface:** the engine is fundamentally synchronous. Async only
  appears at the trace subscription boundary (`TraceSubscriber.StartAsync`,
  `DisposeAsync`, `Channel<T>` reader). `LoadTestHandle.Wait` is a
  blocking join — Python and the CLI both want that.
- **Run gate:** the static `_activeRun` interlock ensures only one run
  per process. The static `QueryRunnerStatus.Instance` and `_logger`
  are only safe because of this. Concurrent runs would interleave
  state.

## Data flow per query

```
user driver task
  └── RunIteration → SemaphoreSlim.Wait → ExecuteQuery
        ├── ActivityIdFactory: Interlocked.Increment(_querySeq) → MakeActivityId(runId, seq)
        ├── new AdomdCommand / set ActivityID property / ExecuteReader
        ├── drain rows (or skip with --skip-results)
        └── return QueryResult
  → status.RecordQuery(r)                              (in-memory counters + percentile reservoir)
  → SubmitTelemetry → telemetryQueue.TryAdd(record)    (unblocks LogWriterLoop)
  → LogWriterLoop: batch drain → StreamWriter         (executions CSV)

(in parallel, server-side)
  AS engine emits ExecutionMetrics with the same ActivityID
        → TraceSubscriber.Events channel
        → trace writer task → trace CSV
```

`persist.py` later JOINs the two CSVs on `(RunId, ActivityID)` to
attach engine CPU/duration to each execution.

## .csproj summary

`net8.0`, `Nullable=enable`, `ImplicitUsings` not enabled (file-by-file
`using`s). One package reference: `Microsoft.AnalysisServices.AdomdClient`.
The heavier `Microsoft.AnalysisServices` (AMO) package is referenced
only by `TraceSubscriber.cs`, but currently lives at the project level —
`TraceModels.cs`'s no-AMO comment is aspirational, not enforced.

## When to split QueryRunner.cs

A future refactor would carve the file into ~4-5 files in the same
namespace (no new sub-namespaces beyond `Tracing/`):

- `Telemetry.cs` ← `QueryResult`, `TelemetryRecord`,
  `QueryRunnerStatus`, `WindowSnapshot`, `SnapshotBox`.
- `Output.cs` ← `QueryRunnerLogger`, `LogWriterLoop`,
  `SanitizeCsvField`, `BuildStats`.
- `Helpers.cs` ← `BuildConnectionString`, `MakeActivityId`,
  `SlotCount`/`NormalizeSlotArray`/`ThrowIfMismatched`/`UserLabel`,
  `ValidateConfig`.
- `QueryRunner.cs` keeps `StartLoadTest` + `RunLoadTestCore` +
  `SimulateUserWithConnections` + `RunIteration` + `ExecuteQuery` +
  `SubmitTelemetry` (the engine).

The line ranges in this doc map directly to that split. Most of the
"public" types in `QueryRunner.cs` today (`QueryRunnerLogger`,
`QueryResult`, `QueryRunnerStatus`, `WindowSnapshot`) are public by
historical accident and would become `internal` in the split, which
also enables `[InternalsVisibleTo("QueryRunner.Tests")]` for unit
tests on the pure helpers (`BuildConnectionString`, `MakeActivityId`,
slot-array helpers, `SanitizeCsvField`, `BuildStats`).

The split is not blocking any current work; this doc exists so that
work isn't blocked on someone re-discovering the structure either.
