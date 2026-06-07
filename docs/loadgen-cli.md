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
| `--users`             | 100     | Number of concurrent virtual users (= number of slots in the per-user queue, = parallel ADOMD connections × `--queries-per-batch`). |
| `--duration`          | 60      | Wall-clock seconds. Each user finishes its current iteration after this elapses; tail can run a few seconds longer. |
| `--queries-per-batch` | 1       | How many queries each user fires concurrently (per iteration). `1` = strictly serial per user. Higher values stress the per-user connection multiplexing. |
| `--ramp-time`         | 30      | Seconds over which to stagger the start of all users (linearly). `0` = all start together (cold-cache hammer).        |
| `--pause-iterations`  | 1000ms  | Sleep between iterations (one full pass through the query list).                                        |
| `--pause-queries`     | 0ms     | Sleep between individual queries within an iteration.                                                   |

### How a "user" iterates

Each slot's loop is:

```
for iter in 0..∞:
  for query in queries:
    fire(query); wait_response(); sleep(pause-queries)
  sleep(pause-iterations)
  exit if duration elapsed
```

When `queries-per-batch > 1`, the inner loop fires the next N queries in
parallel (across N ADOMD connections) and waits for all of them before
moving on.

### Choosing values

- **Capacity planning** ("how many users can this thing serve?"): set
  `--queries-per-batch=1`, ramp slowly (`--ramp-time` ≈ duration/2), pick
  realistic `--pause-iterations` matching real user think time. Sweep
  `--users`.
- **Cold-cache hit-rate measurement**: set `--ramp-time=0` so all users
  pile in at once.
- **Per-user multiplexing stress**: keep `--users` low, raise
  `--queries-per-batch`.

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
  the `LoadTestQueryExecutions` Delta table the notebook persists.
- `./logs/run.trace.csv` — one row per captured trace event (when tracing
  is enabled). Same schema as `LoadTestTraceEvents`.

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
