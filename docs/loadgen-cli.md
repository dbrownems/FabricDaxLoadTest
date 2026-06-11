# LoadGen CLI reference

The `LoadGen` .NET binary is what the notebook spawns as a subprocess and
what you run directly for local tests. Same code path either way; the
notebook just wires JSONL progress on stdout.

```pwsh
dotnet path/to/LoadGen.dll --xmla <endpoint> --dataset <name> \
  --queries-file queries.json --users-file users.json [options]
```

## Required arguments

| Arg              | Description                                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------------------ |
| `--xmla`         | XMLA endpoint URI, e.g. `powerbi://api.powerbi.com/v1.0/myorg/<workspace>` (workspace **display name**, hyphenated form). |
| `--dataset`      | Semantic-model display name (case-sensitive).                                                          |
| `--queries-file` | Path to `queries.json`.                                                                                |
| `--users-file`   | Path to `users.json`. See [docs/impersonation.md](impersonation.md).                                   |

## Authentication

The bearer token is read from (in order): `--token` flag → `--token-file` →
`$PBI_TOKEN` env var. For tokens that the XMLA endpoint accepts, see
[docs/impersonation.md § token acquisition](impersonation.md#token-acquisition-gotcha-local-cli).
Empty token works only for local SSAS / Power BI Desktop integrated auth.

The token is redacted from every log line, error envelope, and exception
message before it leaves the process.

## Workload shape

These are the knobs that control how hard the test pushes the engine.
The model is **closed-loop**: each virtual user issues a query, waits for
the response, then issues the next. There is no fixed-rate firing.

| Arg                   | Default | What it does                                                                                            |
| --------------------- | ------- | ------------------------------------------------------------------------------------------------------- |
| `--users`             | 100     | Number of concurrent virtual users. Each user owns its own ADOMD connection pool and DAX iteration loop. |
| `--duration`          | 60      | Wall-clock seconds. Each user finishes its current iteration after this elapses; tail can run a few seconds longer. |
| `--concurrent-queries-per-user` | 1 | In-flight DAX queries per user. Each user has this many ADOMD connections and a **rolling drain** over the iteration's query list — when any connection finishes, the next pending query is dispatched on the freed connection. This matches Power BI Desktop, which fires up to 6 visual queries concurrently and dispatches the next as each finishes (not in batched all-finish-then-fire-next-batch rounds). `1` = strictly serial per user. |
| `--ramp-time`         | 30      | Seconds over which to stagger the start of all users (linearly). `0` = all start together (cold-cache hammer).        |
| `--pause-iterations`  | 10000ms | Sleep between iterations (one full pass through the query list). Applied per-user once all queries in the iteration have completed. ~10s approximates human dwell time between page interactions; lower it to stress the engine, raise it to model heavier think time. |
| `--pause-queries`     | 0ms     | Sleep on a connection after each individual query completes, before that connection picks up the next pending query. |

### How a "user" iterates

With `--concurrent-queries-per-user=N`, each virtual user runs N
worker tasks sharing one queue per iteration:

```
for iter in 0..∞:
  enqueue all queries
  N workers (each holds 1 ADOMD connection) run in parallel:
    while queue not empty:
      q = dequeue();  fire(q);  wait_response();  sleep(pause-queries)
  await all N workers   ← end of iteration
  sleep(pause-iterations)
  exit if duration elapsed
```

So **a slow query never blocks a fast worker** — the fast worker just
picks up the next pending query and keeps going. The end-of-iteration
barrier exists only so `pause-iterations` think-time is honored once
per pass through the query list.

### Choosing values

- **Capacity planning** ("how many users can this thing serve?"): set
  `--concurrent-queries-per-user=1`, ramp slowly (`--ramp-time` ≈ duration/2),
  pick realistic `--pause-iterations` matching real user think time. Sweep
  `--users`.
- **Cold-cache hit-rate measurement**: set `--ramp-time=0` so all users
  pile in at once.
- **Power-BI-Desktop-like load** (one user opening a report with many
  visuals): keep `--users` low (1-3) and set
  `--concurrent-queries-per-user=6` to match Desktop's per-report
  parallelism cap.

## Other options

| Arg                       | Default       | What it does                                                                  |
| ------------------------- | ------------- | ----------------------------------------------------------------------------- |
| `--replica`               | (default)     | `readonly` to target the read-replica scale-out path, or empty for primary.   |
| `--log-dir`               | `./logs`      | Directory for the executions CSV (and trace CSV when tracing is enabled).     |
| `--log-file`              | auto          | Base filename for the run. Auto: `LoadTest.<users>u.<UTC>.csv`.               |
| `--skip-results`          | false         | Issue `EXECUTE` but discard the resultset rows. Reduces client-side cost when measuring engine-side throughput. |
| `--error-policy`          | `Continue`    | `Continue` (record errors, keep running — recommended) or `Abort` (fail fast on first per-query error). Infrastructure failures still abort regardless. |
| `--no-trace`              | (tracing on)  | Disable XMLA trace subscription. By default LoadGen subscribes to the model's trace, captures `QueryEnd` / `ExecutionMetrics` / `VertiPaqSEQuery*` events for the run, and writes `<log-file>.trace.csv`. Disable if the principal lacks trace permission or the dataset blocks it. |
| `--json-progress`         | false         | Emit JSONL envelopes on stdout (used by the notebook) and route human-readable output to stderr. Use when piping into another process. |
| `--token`                 | –             | Bearer token (else `--token-file` or `$PBI_TOKEN`).                           |

## Output files

For a run with `--log-dir=./logs --log-file=run.csv`:

- `./logs/run.csv` — one row per query execution. Schema is the same as
  the `QueryExecutions` Delta table the notebook persists.
- `./logs/run.trace.csv` — one row per captured trace event (when tracing
  is enabled). Same schema as `TraceEvents`.

Both files are also emitted when `--json-progress` is set; the JSONL
envelopes carry summary stats but the raw per-query rows live in the CSVs.

## Scheduling model — what's NOT supported

LoadGen does not replay recorded session timings. The Power BI Desktop
*Performance Analyzer* JSON shape (`{"version": ..., "events": [...]}`)
**is** accepted as a queries-file, but only the DAX text is extracted —
the `start` / duration fields are ignored. Queries are then fired by the
closed-loop scheduler above, not at their original recorded pace. This
is intentional: replaying one user's exact timing measures "can the
engine keep up with one historical user?" (almost always yes), not "how
many concurrent users can this engine support?", which is the question
load testing should answer.

## Exit codes

| Code | Meaning                                                                |
| ---- | ---------------------------------------------------------------------- |
| 0    | Run completed (errors per `--error-policy`).                           |
| 1    | Bad arguments, file not found, parse error, or fatal exception.        |
| 130  | SIGINT / Ctrl-C — graceful drain completed.                            |
