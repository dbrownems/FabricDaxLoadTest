# QueryRunner — design overview

This is a navigation aid for contributors changing the .NET load-driver
library at `src/QueryRunner/`. It describes the code as it exists today
(not a target). Read it before you edit `QueryRunner.cs` so you can
jump to the right region of the file instead of scrolling 1,250 lines.

> Consumer-facing docs live in [`docs/load-testing-overview.md`](load-testing-overview.md)
> and [`docs/loadgen-main.md`](loadgen-main.md). For the trace
> subscription, see [`docs/tracing.md`](tracing.md).

## What QueryRunner is

A .NET 8 class library that drives a synthetic DAX workload against a
Microsoft Fabric / Power BI XMLA endpoint via ADOMD.NET, captures
per-iteration timings to a CSV, optionally subscribes to a server-side
XMLA trace for engine CPU/duration, and returns a JSON summary. Its only
consumer is `src/LoadGen/`, a thin CLI front-end. The notebook runtime
(`src/fdlt_runtime/`) doesn't load QueryRunner; it runs
`dotnet LoadGen.dll --json-progress` as a subprocess and reads JSONL from
its stdout.

The public surface is intentionally small: one entry point
(`QueryRunner.StartLoadTest`) returning a `LoadTestHandle` that the
caller polls and joins.

## File map

```
src/QueryRunner/
├── QueryRunner.cs        ── ~1250 lines ── StartLoadTest + RunLoadTestCore engine, TelemetryRecord
├── QueryRunnerLogger.cs  ──   110 lines ── internal sealed class QueryRunnerLogger
├── QueryResult.cs        ──    20 lines ── internal sealed class QueryResult
├── QueryRunnerStatus.cs  ──   185 lines ── internal sealed class QueryRunnerStatus + nested WindowSnapshot
├── LoadTestApi.cs        ──   230 lines ── public DTOs + LoadTestHandle
├── IdentityProbe.cs      ──   200 lines ── connects as one user for `LoadGen probe`
├── QueryRunner.csproj    ──    32 lines ── net8.0, nullable enabled, ADOMD.NET ref
└── Tracing/
    ├── FilteredTrace.xmla    ── 186 lines  ── embedded trace definition (events + columns)
    ├── TraceModels.cs        ── 113 lines  ── AsTraceEvent record + TraceColumn/TraceEventClass enums
    └── TraceSubscriber.cs    ── 659 lines  ── XMLA trace subscription
```

`QueryRunner.cs` is still the largest file in the project. The helper
types `QueryRunnerLogger`, `QueryResult` and
`QueryRunnerStatus`/`WindowSnapshot` each live in their own file at the
project root, in the same `FabricDaxLoadTest` namespace, and are
`internal`. `TelemetryRecord` (internal) stays in `QueryRunner.cs`
because the only consumer is the executions-CSV writer that lives there.

## Public types (LoadTestApi.cs)

| Type | Purpose |
|---|---|
| `LoadTestConfig` | Input. Endpoint, dataset, token, queries, slot arrays, durations, error policy, `EnableTracing`. |
| `ErrorPolicy` | `Continue` (default) or `Abort` per-iteration query failure. |
| `LoadTestProgressSnapshot` | Read-only counters + sliding-window stats. Single value the caller polls. |
| `SnapshotBox` | `internal`. Volatile reference cell holding the latest snapshot. |
| `LoadTestHandle` | Returned by `StartLoadTest`. `IsCompleted`, `LatestSnapshot`, `Cancel`, `Wait`, `Dispose`. |

These types, plus `QueryRunner.StartLoadTest`, are the load-test
contract `LoadGen` relies on. `IdentityProbe`/`IdentityProbeResult`
(public, in `IdentityProbe.cs`) back LoadGen's `probe` command.
`TraceSubscriber`/`AsTraceEvent` are also `public` but are used only
inside QueryRunner.

## QueryRunner.cs — sections

`QueryRunner.cs` holds the engine itself plus one internal helper:
`internal class TelemetryRecord` (one executions-CSV row per query
attempt, including retries on reconnect) followed by
`public static class QueryRunner`, the engine.

### Inside `static class QueryRunner`

Search for the symbol names; they're listed in file order.

| Section | What it does |
|---|---|
| static state (`_logger`, `_activeRun`, `_querySeq`) | Process-wide. The `_activeRun` interlock keeps us safe with these. |
| `MakeActivityId(runId, seq)` | Encodes `(runId, seq)` into a deterministic Guid we send as ADOMD `ActivityID`. Lets `persist.py` JOIN executions to `ExecutionMetrics` trace events for engine-CPU back-fill. |
| `StartLoadTest(LoadTestConfig)` | Public entry point. Validates, claims the run gate, allocates `SnapshotBox`, spawns `RunLoadTestCore` on a `Task`, returns `LoadTestHandle`. Synchronous-throws on invalid config or concurrent run. |
| `ValidateConfig` | Argument-shape checks (queries non-empty, slot arrays consistent, endpoint/dataset present, durations positive). |
| **`RunLoadTestCore`** | The actual run. ~580 lines. Phase walkthrough below. |
| `LogWriterLoop` | Background drain of the executions queue into a buffered `StreamWriter` over a `FileStream` opened with `FileShare.Read` so external tailers work mid-run. |
| `SanitizeCsvField` | CSV-escapes / truncates one field. |
| `SimulateUserWithConnections` | Per-user driver loop: iteration → `RunIteration` → think-time pause. Holds the user's connection array for the lifetime of the run. |
| `RunIteration` | Per-iteration query fan-out. `SemaphoreSlim` gate sized to `concurrentQueriesPerUser`, slot-index queue tracks which connection a task uses, transparent reconnect on "connection lost" with a single retry. |
| `SubmitTelemetry` | `QueryResult` → `TelemetryRecord` → enqueue into the CSV writer queue. |
| `ExecuteQuery` | The hot path: open `AdomdCommand`, set `ActivityID`, run the query, drain rows (or skip with `--skip-results`), build `QueryResult`. ADOMD calls are synchronous. |
| `RedactToken` | Strips the bearer token and `Password=` fragments from exception text (used for connect-failure samples and by `IdentityProbe`). |
| `BuildConnectionString` | Assembles the ADOMD connection string (token, EUN, CustomData, Roles, ApplicationName=run-id for trace filtering). |
| Slot-array helpers (`SlotCount`, `ThrowIfMismatched`, `NormalizeSlotArray`, `UserLabel`, `UserEmail`) | Normalize the three impersonation arrays to a common length and label slots. |
| `BuildStats` | Final JSON summary returned by `LoadTestHandle.Wait()`. |

### What `RunLoadTestCore` does, in order

1. **Setup**: destructure config, normalize slot arrays, init the
   linked `CancellationTokenSource` (caller cancel ⨯ duration timer —
   the timer starts here, so trace setup, warm-up and ramp count toward
   `DurationSeconds`), create `_logger`, seed the initial snapshot, open
   the executions CSV + start `LogWriterLoop`.
2. **Trace subscription**: if `EnableTracing && logDirectory`,
   create a `TraceSubscriber` (keeps only trace rows whose
   `ApplicationName` is `FabricDaxLoadTest/<runId>`, filtering
   client-side), open the trace CSV, and start a writer task draining
   `subscriber.Events` into the file. Failures here are warnings, not
   fatal — except the `OnFatalError` callback, which sets
   `traceFatalError` and cancels the run. See [tracing.md](tracing.md).
3. **Threadpool warmup**: `ThreadPool.SetMinThreads` sized for
   `users × concurrentQueriesPerUser` so the .NET injection rate does
   not serialize ramp-up on small Fabric notebook hosts.
4. **Pre-warm connection**: a single up-front `Open()` against
   slot 0 to absorb the gateway/model cold-start (50-100s on a cold
   capacity) before per-user opens hit the front-end.
5. **Ramp**: one task per user, scheduled with `rampIntervalMs`
   delay. Each task opens its `concurrentQueriesPerUser` connections,
   bumps `connectedUsers` + `status.IncrementActiveUsers`, then jumps
   into `SimulateUserWithConnections`. The main thread loops printing
   ramp progress every `nUsers/10` connections.
6. **Snapshot publisher**: 1 Hz background task that reads
   `QueryRunnerStatus`, computes QPS over the last ~1 s sample and
   duration percentiles over the last 5 s, and writes the result into
   `SnapshotBox` so polling callers see live progress (LoadGen emits each
   snapshot as a JSONL `progress` envelope).
7. **Steady-state**: `Task.WaitAll(userTasks)`. The 60 s
   periodic reporter runs in parallel.
8. **Drain & shutdown**: cancel the snapshot publisher, wait
   for the periodic reporter, complete the executions queue and join
   the writer, give the trace 5 s to flush in-flight `ExecutionMetrics`
   events, dispose the `TraceSubscriber`, resolve the final phase
   (`Failed > Cancelled > Done`), publish the final snapshot, dispose
   the logger.
9. **Return**: the JSON string built by `BuildStats`. The
   `LoadTestHandle.Wait()` caller receives this.

## Tracing/

`TraceModels.cs` defines `AsTraceEvent` — the row shape the trace
subscriber emits and the trace CSV stores — plus local copies of the
`TraceColumn` and `TraceEventClass` enums, so the project depends only
on the ADOMD client package, not AMO.

`TraceSubscriber.cs` creates a database-scoped trace by sending XMLA
`<Create>` / `<Subscribe>` / `<Delete>` commands over ADOMD (no AMO),
using the embedded `FilteredTrace.xmla` definition. It filters rows
client-side by `ApplicationName` (the PBI Service rejects server-side
filters on subscription traces), passes them through a bounded
`Channel<AsTraceEvent>`, and surfaces them via `Events`. `OnFatalError`
is invoked when the trace reader fails non-recoverably mid-run; the run
aborts cleanly. Drain semantics live in `DisposeAsync`. The constraints
behind this design are in [tracing.md](tracing.md).

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
  takes effect before the next query starts, and in-flight queries run
  to completion.
- **Async surface:** the engine is fundamentally synchronous. Async only
  appears at the trace subscription boundary (`TraceSubscriber.StartAsync`,
  `DisposeAsync`, `Channel<T>` reader). `LoadTestHandle.Wait` is a
  blocking join, which is what the CLI wants.
- **Run gate:** the static `_activeRun` interlock ensures only one run
  per process. The static `QueryRunnerStatus.Instance` and `_logger`
  are only safe because of this. Concurrent runs would interleave
  state.

## Data flow per query

```
user driver task
  └── RunIteration → SemaphoreSlim.Wait → ExecuteQuery
        ├── Interlocked.Increment(_querySeq) → MakeActivityId(runId, seq)
        ├── new AdomdCommand / set ActivityID property / ExecuteReader
        ├── drain rows (or skip with --skip-results)
        └── return QueryResult
  → status.RecordQuery(r)                              (in-memory counters + rolling duration queue)
  → SubmitTelemetry → telemetryQueue.TryAdd(record)    (unblocks LogWriterLoop)
  → LogWriterLoop: batch drain → StreamWriter         (executions CSV)

(in parallel, server-side)
  AS engine emits ExecutionMetrics with the same ActivityID
        → TraceSubscriber.Events channel
        → trace writer task → trace CSV
```

`persist.py` later decodes `QuerySeq` from each `ExecutionMetrics`
row's ActivityID and joins on `(RunId, QuerySeq)` to attach engine
CPU/duration to each execution.

## .csproj summary

`net8.0`, `Nullable=enable`, `ImplicitUsings` not enabled (file-by-file
`using`s). One package reference: `Microsoft.AnalysisServices.AdomdClient`
(no AMO). `FilteredTrace.xmla` is an embedded resource.

## When to split QueryRunner.cs further

The helper types (`QueryRunnerLogger`, `QueryResult`,
`QueryRunnerStatus`+`WindowSnapshot`) have already been moved to
sibling files. A future refactor could carve the engine itself:

- `Helpers.cs` ← `BuildConnectionString`, `MakeActivityId`,
  `SlotCount`/`NormalizeSlotArray`/`ThrowIfMismatched`/`UserLabel`,
  `ValidateConfig`, `SanitizeCsvField` — pure-ish, easy to unit test.
- `Output.cs` ← `LogWriterLoop`, `BuildStats`, `TelemetryRecord`.
- `QueryRunner.cs` keeps `StartLoadTest` + `RunLoadTestCore` +
  `SimulateUserWithConnections` + `RunIteration` + `ExecuteQuery` +
  `SubmitTelemetry` (the engine).

That split would also enable `[InternalsVisibleTo("QueryRunner.Tests")]`
for unit tests on the pure helpers (`BuildConnectionString`,
`MakeActivityId`, slot-array helpers, `SanitizeCsvField`,
`BuildStats`).

Not blocking any current work; this doc exists so that work isn't
blocked on someone re-discovering the structure either.
