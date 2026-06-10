# Reports

Power BI reports for visualizing FabricDaxLoadTest results.

## LoadTestsOverview

DirectQuery report against the Fabric Lakehouse SQL analytics endpoint
where `fdlt_runtime` writes its Delta tables (`LoadTests`, `LoadTestRuns`,
`QueryExecutions`, `TraceEvents`).

### First-time setup (per developer)

1. **Open the project** — double-click `LoadTestsOverview.pbip` in Power BI
   Desktop. The model loads with placeholder parameter values that will
   fail with a clear error.

2. **Set your three parameters** — Home → Transform data → Edit parameters:

   | Parameter | What to enter | Example |
   |---|---|---|
   | `ServerName` | SQL endpoint hostname from your lakehouse's *Settings → SQL analytics endpoint* | `xxxxx-yyyyy.datawarehouse.fabric.microsoft.com` |
   | `DatabaseName` | Lakehouse name (the database name shown in the SQL endpoint) | `LoadTests` |
   | `SchemaName` | `dbo` for non-schema-enabled lakehouses, or the schema you wrote to | `dbo` |

3. **Tell git to ignore your local parameter values** so they don't get
   committed back. From the repo root:

   ```pwsh
   git update-index --skip-worktree `
     Reports/LoadTestsOverview.SemanticModel/definition/expressions.tmdl
   ```

   After this, `git status` and `git diff` will not show your edits to that
   file, and `git commit -a` will skip it. This is **per-clone** — every
   developer who clones the repo runs it once.

   To temporarily un-skip (e.g. to pull upstream changes to that file):
   ```pwsh
   git update-index --no-skip-worktree `
     Reports/LoadTestsOverview.SemanticModel/definition/expressions.tmdl
   git pull
   # re-enter your parameter values, then:
   git update-index --skip-worktree `
     Reports/LoadTestsOverview.SemanticModel/definition/expressions.tmdl
   ```

   To check what's currently skip-worktree:
   ```pwsh
   git ls-files -v | Where-Object { $_ -clike 'S *' }
   ```

### What's in the model

* **4 DirectQuery tables**, related on the natural keys
  (`LoadTestRuns.LoadTestId → LoadTests`, `QueryExecutions.SourceId → LoadTestRuns`,
  `TraceEvents.SourceId → LoadTestRuns`). `QueryHash`, `QueryShapeHash`,
  and `QueryText` are columns on `QueryExecutions` directly.
* **Friendly measures on `QueryExecutions`** with the raw timing columns
  hidden: `Total Executions`, `Successful Executions`, `Failed Executions`,
  `QPS`, `Avg/P50/P95/P99/Max Latency (ms)`, `Engine CPU (s)`, `SE CPU (s)`,
  `FE CPU (s)`, `Engine Duration (s)`, `Total Engine CPU (ms)`,
  `Active Users (max)`.
* **Run-level passthrough measures on `LoadTestRuns`**: `Run QPS`,
  `Run P50 (ms)`, `Run P95 (ms)` — read straight from the run summary
  written by `fdlt_runtime`.

### Pages

* **Load Test Overview** — slicer to pick a single run; KPI cards for
  Successful Executions / QPS / Avg & P95 Latency; latency, QPS,
  active-users, and engine-CPU-per-second time series; Top Queries by
  Engine CPU table.

### Files in this folder

```
Reports/
  .gitignore                              # ignores .pbi/ caches and *.local files
  LoadTestsOverview.pbip                  # project entry point
  LoadTestsOverview.SemanticModel/        # TMDL model (DirectQuery)
    .platform
    definition.pbism
    definition/
      database.tmdl
      model.tmdl
      expressions.tmdl                    # ServerName / DatabaseName / SchemaName
      relationships.tmdl
      cultures/en-US.tmdl
      tables/
        LoadTests.tmdl
        LoadTestRuns.tmdl
        QueryExecutions.tmdl              # all the friendly-named measures
        TraceEvents.tmdl
  LoadTestsOverview.Report/               # PBIR report
    .platform
    definition.pbir
    definition/
      report.json
      version.json
      pages/
        pages.json
        LoadTestOverview.Page/
          page.json
          visuals/...
```
