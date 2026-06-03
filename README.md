# FabricDaxLoadTest

A load testing tool for Microsoft Fabric and Power BI semantic models. Simulates concurrent users executing DAX queries via the XMLA endpoint using ADOMD.NET.

Designed to run **inside a Fabric notebook** — no Spark cluster, no separate VM, no `dotnet build` required for end users. The notebook downloads a pre-built `QueryRunner.dll` from a GitHub release and drives it via [pythonnet](https://pythonnet.github.io/).

## Why another load test tool?

The existing [FabricLoadTestTool](https://github.com/microsoft/fabric-toolbox/tree/main/tools/FabricLoadTestTool) is pure-Python and uses the public REST API. That works, but has limitations:

- No realistic per-user XMLA connection cost (TCP/TLS handshake, model attach)
- Limited to whatever the REST API path exposes
- Can't easily simulate Row-Level Security via `CustomData` / `Roles`
- Per-thread Python overhead caps achievable concurrency

This tool uses **ADOMD.NET** through pythonnet, so each simulated user gets a real XMLA connection — the same path Power BI Desktop, Excel, and Tabular Editor use. That makes the test results meaningful for capacity planning.

## Components

| Piece | What it is |
|---|---|
| `QueryRunner.dll` | .NET 8 library: orchestrates concurrent users, opens ADOMD.NET connections, runs DAX, writes per-query telemetry CSV. |
| `LoadGen.exe` | Thin .NET CLI wrapper around `QueryRunner` — useful for local runs against PBI in your tenant. |
| `notebooks/FabricDaxLoadTest.ipynb` | The headline. Drop into a Fabric workspace, edit four parameter cells, run. |

## Quick start (Fabric notebook)

1. Download the latest `notebooks/FabricDaxLoadTest.ipynb` from this repo.
2. In your Fabric workspace, **Import notebook → From local file**.
3. Attach a default Lakehouse (any will do — used as a working dir for the DLL and CSV logs).
4. Edit the parameters cell at the top: `WORKSPACE`, `DATASET`, `QUERIES`, `USERS`, `DURATION_SEC`, `N_USERS`.
5. Run all cells. The notebook will:
   - Download the latest `QueryRunner.dll` release into `/lakehouse/default/Files/loadtest/`
   - Acquire a Power BI bearer token via `notebookutils.credentials.getToken('pbi')`
   - Spin up `N_USERS` concurrent ADOMD.NET connections
   - Run for `DURATION_SEC`, log every query to a CSV
   - Render a ramp chart and latency percentiles

## Quick start (local CLI)

Requires the [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0).

```bash
git clone https://github.com/dbrownems/FabricDaxLoadTest.git
cd FabricDaxLoadTest
dotnet build -c Release

# Edit samples/users.json and samples/queries.json for your model
dotnet run --project src/LoadGen -c Release -- \
  --xmla "powerbi://api.powerbi.com/v1.0/myorg/MyWorkspace" \
  --dataset "My Semantic Model" \
  --queries-file samples/queries.json \
  --users-file samples/users.json \
  --users 50 --duration 120 --ramp-time 30 \
  --auth browser
```

## Inputs

### `queries.json`
Array of DAX query strings. Each user iterates through the entire list per "iteration":
```json
[
  "EVALUATE ROW(\"x\", COUNTROWS('Sales'))",
  "EVALUATE TOPN(10, SUMMARIZECOLUMNS('Date'[Year], \"Total\", [Sales Amount]))"
]
```

### `users.json`
Array of `{email, role}` objects used for RLS impersonation via `CustomData` and `Roles`. The interactive token holder needs the appropriate "Test as role" privilege; otherwise leave `role` empty:
```json
[
  { "email": "alice@contoso.com", "role": "Sales US" },
  { "email": "bob@contoso.com",   "role": "Sales EU" }
]
```

If `--users` exceeds the array length, entries are reused round-robin.

## CLI options

| Option | Default | Description |
|---|---|---|
| `--xmla` | *(required)* | XMLA endpoint URL (`powerbi://api.powerbi.com/v1.0/myorg/Workspace`) |
| `--dataset` | *(required)* | Semantic model name |
| `--queries-file` | *(required)* | Path to queries.json |
| `--users-file` | *(required)* | Path to users.json |
| `--duration` | 60 | Test duration in seconds |
| `--users` | 100 | Concurrent simulated users |
| `--ramp-time` | 30 | Seconds to ramp from 0 → `--users` |
| `--queries-per-batch` | 1 | Concurrent queries per user (use >1 to stress engine concurrency without adding users) |
| `--pause-iterations` | 1000 | Pause between iterations (ms) |
| `--pause-queries` | 0 | Pause after each query (ms) |
| `--replica` | "" | `readonly` to target the scale-out read replica |
| `--skip-results` | false | Drain response without parsing rows (lower client-side cost) |
| `--log-dir` | ./logs | Directory for telemetry CSV |
| `--auth` | — | Azure auth: `browser`, `cli`, `devicecode`, `default`, `env`, `managedidentity` |
| `--token-file` | — | File containing bearer token |
| `--token` | — | Inline bearer token (avoid; use `--token-file` or `--auth`) |

The token must be scoped for `https://analysis.windows.net/powerbi/api`.

## Output

### Console
Live progress with per-minute stats during the run, then a JSON summary with overall stats, latency percentiles (p50/p95/p99), per-user breakdown, and sample errors.

### CSV telemetry
One row per query, written to `--log-dir`:
```
QueryNumber,UserEmail,Timestamp,StartTimeMs,DurationMs,Outcome,MessageText,ActiveUsers
0,alice@contoso.com,2026-06-03 18:46:08.557,3150,45.2,Success,"1 rows",50
```

The notebook reads this CSV back into a pandas DataFrame and renders charts.

## How it scales

Two key engineering details that took some pain to land:

1. **ThreadPool pre-warm.** A 2-core Fabric Python notebook host has `MinThreads = 2` by default and grows the worker pool at ~1 thread/sec. With 100 sync-blocking ADOMD.NET drivers, ramp serializes to ~100 seconds. `QueryRunner` calls `ThreadPool.SetMinThreads(nUsers + 32, ...)` up front, so workers are eager-allocated and ramp follows the configured `--ramp-time`.

2. **Pre-warm connection.** The first connection to a cold model pays the engine cold-start (50–100 s on a cold capacity). `QueryRunner` opens one warmup connection on the main thread before launching user tasks, so per-user `Open()` times reflect socket cost only — clean numbers for capacity planning.

## Building from source

```bash
dotnet build -c Release                               # build everything
dotnet publish src/LoadGen -c Release -r win-x64      # self-contained LoadGen.exe
```

The QueryRunner DLL ends up at `src/QueryRunner/bin/Release/net8.0/QueryRunner.dll` along with `Microsoft.AnalysisServices.AdomdClient.dll` and friends — copy them all to your Fabric lakehouse if you want to use a local build instead of the GitHub release.

## License

[MIT](LICENSE).
