# Engine tracing

During a load test, LoadGen subscribes to a server-side **XMLA trace** on
the semantic model and writes the engine's events to `<log-file>.trace.csv`.
The notebook loads that file into the `TraceEvents` table and uses it to
add engine CPU and duration to every row of `QueryExecutions`
(see [data-dictionary.md](data-dictionary.md)).

This page records how the subscription works and the Power BI Service
constraints behind its design. Read it before you change
`src/QueryRunner/Tracing/TraceSubscriber.cs` or `FilteredTrace.xmla`.

Tracing is on by default. Turn it off with `ENABLE_TRACING = False` in the
notebook or `--no-trace` on the CLI.

- [Captured events](#captured-events)
- [Correlation](#correlation)
- [Lifecycle](#lifecycle)
- [Power BI Service constraints](#power-bi-service-constraints)
- [Known limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)

## Captured events

The event and column list lives in the embedded resource
`src/QueryRunner/Tracing/FilteredTrace.xmla`.

| ID | Event | Used for |
|---|---|---|
| 136 | `ExecutionMetrics` | One per query. Its JSON `TextData` supplies `EngineDurationMs`, `EngineCpuMs`, `SECpuMs`, `FECpuMs`, `ExecutionDelayMs`, `CapacityThrottlingMs` and `PeakMemoryKB`; the notebook's CPU chart also reads it. |
| 99 | `DirectQueryEnd` | One per DirectQuery push-down. Aggregated into `DirectQueryCount`, `DirectQueryDurationMs` and `DirectQueryCpuMs`. Only DirectQuery and composite models — or Direct Lake queries that fall back to DirectQuery — emit it. |
| 98 | `DirectQueryBegin` | Stored in `TraceEvents` only. |
| 15, 16 | `CommandBegin`, `CommandEnd` | Stored in `TraceEvents` only. |
| 17 | `Error` | Stored in `TraceEvents` only. |

Not captured on purpose: `QueryBegin`/`QueryEnd` (9/10), the VertiPaq
storage-engine events (82/83/85), `DAXQueryPlan` (112) and `JobGraph`
(134). They multiplied the trace volume per query (about 14 events per
query at one point; an import-model query now produces about 1), and at
200 users the server-side trace buffer overflowed after about 3.5
minutes. When that happens AS silently stops emitting events for the
rest of the run — in that test about 25% of late-run queries lost their
engine metrics. `ExecutionMetrics` alone supplies what `QueryEnd` used to.

## Correlation

The trace has no client-side identity, so LoadGen gives every query a
deterministic **ActivityID**:

- `QueryRunner.MakeActivityId(runId, seq)` builds a GUID whose first
  12 bytes come from an MD5 hash of the RunId and whose last 4 bytes are
  `seq` in big-endian order. `seq` (`QuerySeq`) is a per-run counter,
  incremented for every execution attempt, retries included.
- It is set with the typed property `AdomdCommand.ActivityID`. The
  Power BI Service rejects the same value sent as an XMLA property
  (`Properties.Add(new AdomdProperty("ActivityID", …))`) with "The
  'ActivityID' property was not recognized".
- AS copies it to ColumnID 46 of the `ExecutionMetrics` row. `persist.py`
  takes the last 8 hex characters of that GUID, decodes `QuerySeq`, and
  joins on `(RunId, QuerySeq)`. `analyze.py` uses the same rows for the
  CPU chart.
- `DirectQueryEnd` rows don't carry the ActivityID, so they're grouped
  by `RequestId` and attached through the `ExecutionMetrics` row that has
  the same `RequestId`.

`SessionID` (ColumnID 39) isn't populated on `ExecutionMetrics`, which
is why `QueryExecutions.LogicalSessionId` is synthesized instead.

## Lifecycle

`TraceSubscriber` runs in the LoadGen process, alongside the virtual
users:

1. **Open + Create** — on the calling thread, before any user connects.
   A dedicated `AdomdConnection` (Application Name
   `FabricDaxLoadTest.Trace`) sends `<Create><Trace>` with the
   embedded definition, renamed to a per-run `FDLT-<random id>`. Every
   Create, Subscribe and Delete command sets the `Catalog` property.
2. **Subscribe** — `ExecuteReader` on `<Subscribe>` blocks until the
   first event arrives, so the reader runs on a dedicated
   (`LongRunning`) thread rather than a thread-pool worker. Under load
   the pool is saturated by the virtual users; a starved reader lets
   the server-side buffer fill up, and AS then stops emitting events.
3. **Filter** — each row is kept only if its `ApplicationName` is
   `FabricDaxLoadTest/<RunId>`, the value every load-test connection
   sets. Rows whose `ApplicationName` is NULL are kept too.
4. **Buffer and write** — kept rows go through a bounded channel
   (10,000 events) to a writer task that appends them to the trace CSV.
5. **Drain** — after the users stop, LoadGen waits 5 seconds for
   in-flight queries' `ExecutionMetrics` events (they arrive up to about
   3 seconds late under load). The wait is skipped when the run was
   cancelled.
6. **Dispose** — cancels the reader (waits up to 5 seconds), then sends
   `<Delete>` on a fresh connection, bounded to 15 seconds, because the
   subscribe connection may still be busy.

If the trace can't be created, the run continues without engine
metrics and a warning is logged. If the reader fails during the run,
the run is aborted and LoadGen exits with an error, so a run is never
reported as successful with silently missing trace data.

## Power BI Service constraints

These are the reasons behind the design above. Each was learned the hard
way against the Power BI Service and Fabric XMLA endpoints.

- **Only database-scoped traces.** Server-scoped (AMO) traces fail with
  "Permission to create a trace is denied. Only members of the server
  administrator role can create or subscribe to traces without a filter
  tree." LoadGen connects with `Initial Catalog=<model>` and sets the
  `Catalog` command property on every trace command (the pattern from
  [dbrownems/XmlaMonitorSample](https://github.com/dbrownems/XmlaMonitorSample)).
- **No server-side `<Filter>`.** The service rejects filter clauses on
  subscription traces, so the trace definition has none and filtering
  by `ApplicationName` happens client-side (step 3).
- **Create on the calling thread.** Running Open + Create on a
  background `Task.Run` thread made the service answer the following
  Subscribe with a non-rowset response
  (`AdomdUnknownResponseException`: "The result set returned by the
  server is not a rowset.").
- **Request `EventClass` (ColumnID 0) on every event.** The subscribe
  reader returns the union of all requested columns; without
  `EventClass` a row can't be mapped back to its event and is dropped.
  Trace definitions exported from SQL Server Profiler omit it, so don't
  paste one in unchanged.
- **Never request ColumnID 44 (`RequestParameters`) or 45
  (`RequestProperties`).** The service sends them as list-shaped
  BinaryXml that ADOMD can't parse; the reader fails mid-stream with
  "Lists of BinaryXml value tokens not supported." and the whole
  subscription dies.
- **Some event/column pairs are rejected.** Create can fail with "The
  event Id=N does not contain the column Id=M". `CreateTraceWithAutoRetry`
  removes that column from the definition, deletes any half-created
  trace, and retries — up to 32 times.
- **Keep the event set small.** See [Captured events](#captured-events):
  the server-side buffer is finite and overflows silently.

## Known limitations

These are documented here until they're fixed:

- **`TextData` is truncated to 500 characters** in the trace CSV. Import
  models' `ExecutionMetrics` JSON is well under that (332 characters at
  most in our tests), but a longer payload — for example a DirectQuery
  source query in `DirectQueryEnd`, or a richer `ExecutionMetrics`
  document — is cut off. A truncated `ExecutionMetrics` document can't be
  parsed, and that query's engine columns stay NULL.
- **Dropped events aren't counted.** When the writer falls 10,000 events
  behind, new events are discarded (the channel is in `DropWrite` mode),
  but `TryWrite` still reports success, so `EventsDroppedByBackpressure`
  stays 0 and the dropped events are counted as seen.
- **Rows with a NULL `ApplicationName` bypass the run filter.** Every
  captured event requests the column, so these should be rare, but such
  rows aren't proven to belong to the run.
- **The trace isn't deleted on a hard kill.** If LoadGen is killed (for
  example by Ctrl+C in the CLI's default console mode), the `<Delete>`
  isn't sent and the trace is left for the service to clean up.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `WARNING: Trace subscription failed (run will continue without engine telemetry)` | The identity isn't allowed to create a trace on this model, or the model blocks traces. The warning includes the server's message. Run with tracing off, or use an identity that may trace the model. |
| `Lists of BinaryXml value tokens not supported.` | A requested column returns list-shaped BinaryXml. Remove its `<ColumnID>` from `FilteredTrace.xmla` (ColumnIDs 44 and 45 are the known offenders). |
| `The result set returned by the server is not a rowset.` | Open + Create ran on a background (`Task.Run`) thread. Keep them on the calling thread (see [Power BI Service constraints](#power-bi-service-constraints)). |
| `Trace subscription failed mid-run; load test aborted.` | The reader failed during the run. The preceding `[TraceSubscriber FATAL]` line on stderr has the server's message. |
| Engine columns are NULL for some queries | Their `ExecutionMetrics` events arrived after the drain, were dropped, or had truncated `TextData` (see [Known limitations](#known-limitations)). |
| `DirectQuery*` columns are NULL | Expected for import and Direct Lake models that don't fall back to DirectQuery. |
