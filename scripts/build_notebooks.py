"""Generate notebooks/Run.ipynb and notebooks/Queries.ipynb.

The notebooks are deployed into a `LoadTests` workspace folder by
scripts/Deploy-LoadTests.ps1, alongside a `LoadTests` lakehouse that
holds QueryRunner.dll under Files/bin/ and run logs under Files/runs/.

The notebooks self-discover the workspace + lakehouse at run time via
`notebookutils.runtime.context`, so they are workspace-portable and do
not need rewriting per deployment.

Run from repo root:
    python scripts\\build_notebooks.py
"""
import json
import nbformat
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT  = REPO / "notebooks"
OUT.mkdir(exist_ok=True, parents=True)

FABRIC_NB_METADATA = {
    "language_info": {"name": "python"},
    "kernel_info":   {"name": "jupyter", "jupyter_kernel_name": "python3.11"},
    "kernelspec":    {"display_name": "Jupyter", "language": None, "name": "jupyter"},
    "microsoft":     {"language": "python", "language_group": "jupyter_python"},
    "dependencies":  {},
    "spark_compute": {"compute_id": "/trident/default"},
}


def new_notebook():
    nb = nbformat.v4.new_notebook()
    nb.metadata = {**FABRIC_NB_METADATA}
    return nb


def md(nb, text):
    nb.cells.append(nbformat.v4.new_markdown_cell(text.strip("\n")))


def code(nb, text):
    nb.cells.append(nbformat.v4.new_code_cell(text.strip("\n")))


def patch_for_github(nb):
    """nbformat fields GitHub's strict renderer requires; Fabric is lenient."""
    for c in nb.cells:
        if c.cell_type == "code":
            c["execution_count"] = None
            c["outputs"]         = []
            c["metadata"]        = {}


def write(nb, path: Path):
    patch_for_github(nb)
    with open(path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
    nbformat.validate(nbformat.read(str(path), as_version=4))
    print(f"OK: {path.relative_to(REPO)}  ({path.stat().st_size:,} bytes, {len(nb.cells)} cells)")


# ────────────────────────────────────────────────────────────────────────────
# Run.ipynb — the runner
# ────────────────────────────────────────────────────────────────────────────
def build_run():
    nb = new_notebook()

    md(nb, r"""
# FabricDaxLoadTest — Run

Drives concurrent DAX queries against a Power BI / Fabric semantic model via
the **XMLA endpoint** using `QueryRunner.dll` (loaded in-process through
pythonnet).

This notebook lives in the workspace folder **`LoadTests`** alongside the
**`LoadTests`** lakehouse, which holds:

- `Files/bin/`   — `QueryRunner.dll` + ADOMD client dependencies
- `Files/runs/`  — per-run telemetry CSVs (created on first run)
- `Files/queries.json` — the corpus of DAX queries (managed via `Queries.ipynb`)

## How to use

1. Edit cell **1** to point at the target workspace + dataset and tweak load
   parameters.
2. **Run All**. Cell **5** prints a live status line every second; press
   **Interrupt Kernel** (■) to cancel — the run drains cleanly.
3. Cell **6** plots latency / QPS / users from the per-run CSV.

> Re-deploy / upgrade the bits in `Files/bin/` by re-running
> `scripts/Deploy-LoadTests.ps1` from a clone of the repo.
""")

    # 1. Configuration
    code(nb, r"""
# ── 1. Configuration ──────────────────────────────────────────────────────────
TARGET_WORKSPACE = "MyWorkspace"        # workspace hosting the semantic model
TARGET_DATASET   = "My Semantic Model"  # semantic model display name

DURATION_SECONDS         = 60
CONCURRENT_USERS         = 25
QUERIES_PER_BATCH        = 1
PAUSE_BETWEEN_ITERATIONS_MS = 1000
PAUSE_BETWEEN_QUERIES_MS    = 0
USER_RAMP_TIME_SEC       = 15
TARGET_REPLICA           = ""       # "readonly" → scale-out read replica
SKIP_RESULTS             = False    # True drains rows without parsing

# Optional inline override; if empty, we read Files/queries.json from this
# notebook's lakehouse.
QUERIES_INLINE = []     # e.g. ["EVALUATE ROW(\"x\", 1)"]

# Optional inline users; if empty, all virtual users share the interactive
# token's identity (no role / CustomData impersonation). Each entry is
# {"email": "...", "role": "..."} — the role string is forwarded to the AS
# `Roles=` connection string property.
USERS_INLINE = []
""")

    # 2. Auto-discover lakehouse + load QueryRunner.dll
    code(nb, r"""
# ── 2. Mount the LoadTests lakehouse and load QueryRunner.dll ────────────────
# This notebook lives in the workspace folder `LoadTests`. The companion
# lakehouse (also `LoadTests`) is in the same folder and holds the assemblies.
import os, glob, json
import notebookutils

ctx = notebookutils.runtime.context
WS_ID   = ctx["currentWorkspaceId"]
WS_NAME = ctx.get("currentWorkspaceName", WS_ID)

# Resolve LoadTests lakehouse — friendly-name support is disabled on some
# OneLake tenants, so we look up the GUID via the Fabric items API and use
# that in the abfss path. We hit REST directly (rather than via
# sempy.fabric.list_items) because importing sempy.fabric here pre-initializes
# pythonnet and breaks namespace registration for our path-loaded assemblies
# in the next cell.
LAKEHOUSE_NAME = "LoadTests"
import requests
_tok = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
_r = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/items?type=Lakehouse",
    headers={"Authorization": f"Bearer {_tok}"}, timeout=30)
_r.raise_for_status()
_match = [i for i in _r.json().get("value", []) if i["displayName"] == LAKEHOUSE_NAME]
if not _match:
    raise RuntimeError(f"Lakehouse '{LAKEHOUSE_NAME}' not found in workspace {WS_ID}")
LH_ID = _match[0]["id"]
LH_ABFSS = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}"

# Stage QueryRunner.dll + dependencies to a local /tmp dir. pythonnet's
# AssemblyResolver wants a real filesystem path; abfss:// won't do.
# We list and copy per-file (recursing manually) because notebookutils.fs.cp
# with recurse=True does a HEAD/getStatus on the source directory which
# OneLake currently rejects with HTTP 400 for Files/* directories.
STAGE = "/tmp/fdlt-bin"
os.makedirs(STAGE, exist_ok=True)

def _stage_dir(src_abfss, dst_local):
    os.makedirs(dst_local, exist_ok=True)
    for entry in notebookutils.fs.ls(src_abfss):
        name = entry.name
        sp   = f"{src_abfss.rstrip('/')}/{name}"
        dp   = os.path.join(dst_local, name)
        if entry.isDir:
            _stage_dir(sp, dp)
        else:
            notebookutils.fs.cp(sp, f"file://{dp}")

_stage_dir(f"{LH_ABFSS}/Files/bin", STAGE)

dll = os.path.join(STAGE, "QueryRunner.dll")
if not os.path.exists(dll):
    # Fallback search in case the layout changes.
    cands = glob.glob(os.path.join(STAGE, "**", "QueryRunner.dll"), recursive=True)
    dll = cands[0] if cands else dll
if not os.path.exists(dll):
    raise FileNotFoundError(
        f"QueryRunner.dll not found under {STAGE}. Re-run Deploy-LoadTests.ps1 "
        f"to populate {LH_ABFSS}/Files/bin."
    )
print(f"Workspace : {WS_NAME} ({WS_ID})")
print(f"Lakehouse : {LAKEHOUSE_NAME} ({LH_ID})")
print(f"DLL       : {dll}  ({os.path.getsize(dll):,} bytes)")
""")

    # 3. pythonnet bootstrap
    code(nb, r"""
# ── 3. pythonnet bootstrap ────────────────────────────────────────────────────
# Add sempy's bundled .NET libs to sys.path so clr can find
# Microsoft.AnalysisServices.AdomdClient by simple name. Then AddReference
# ADOMD first (warms the AppDomain), then QueryRunner.dll by full path.
# `import clr` raises RuntimeError (not ImportError) on Fabric Linux when
# coreclr isn't preloaded, so the except must be broad.
import sys
sempy_lib = "/home/trusted-service-user/cluster-env/trident_env/lib/python3.11/site-packages/sempy/lib"
if sempy_lib not in sys.path:
    sys.path.insert(0, sempy_lib)

try:
    import clr
except Exception:
    from pythonnet import load
    load("coreclr")
    import clr

clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
clr.AddReference(dll)
from FabricDaxLoadTest import QueryRunner, LoadTestConfig          # noqa: E402
from System import Array, String                                   # noqa: E402
import System.Reflection                                           # noqa: E402
print(f"QueryRunner v{System.Reflection.Assembly.GetAssembly(QueryRunner).GetName().Version}")
""")

    # 4. Build LoadTestConfig
    code(nb, r"""
# ── 4. Build the LoadTestConfig ───────────────────────────────────────────────
import time, uuid
from datetime import datetime, timezone

# Queries
if QUERIES_INLINE:
    queries = list(QUERIES_INLINE)
else:
    qpath = f"{LH_ABFSS}/Files/queries.json"
    raw = notebookutils.fs.head(qpath, 1024 * 1024 * 4)   # up to 4 MB
    queries = [q if isinstance(q, str) else q["query"] for q in json.loads(raw)]
print(f"Queries : {len(queries)}")

# Users (round-robin to CONCURRENT_USERS)
if USERS_INLINE:
    base = USERS_INLINE
else:
    base = [{"email": "anonymous@local", "role": ""}]
users = [base[i % len(base)] for i in range(CONCURRENT_USERS)]

# Run output dir under Files/runs/<runId>/
RUN_ID    = uuid.uuid4().hex[:8]
RUN_LOCAL = f"/tmp/fdlt-run-{RUN_ID}"
os.makedirs(RUN_LOCAL, exist_ok=True)
LOG_FILE  = f"LoadTest.{CONCURRENT_USERS}u.{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.csv"

# Token
TOKEN = notebookutils.credentials.getToken("pbi")

# Build the config
xmla = f"powerbi://api.powerbi.com/v1.0/myorg/{TARGET_WORKSPACE}"
if TARGET_REPLICA:
    xmla = f"{xmla}?{TARGET_REPLICA}"

cfg = LoadTestConfig()
cfg.Queries                  = Array[String]([str(q) for q in queries])
cfg.XmlaEndpoint             = xmla
cfg.Dataset                  = TARGET_DATASET
cfg.Token                    = TOKEN
cfg.UserEmails               = Array[String]([u["email"] for u in users])
cfg.UserRoles                = Array[String]([u.get("role", "") for u in users])
cfg.DurationSeconds          = DURATION_SECONDS
cfg.QueriesPerBatch          = QUERIES_PER_BATCH
cfg.PauseBetweenIterationsMs = PAUSE_BETWEEN_ITERATIONS_MS
cfg.PauseBetweenQueriesMs    = PAUSE_BETWEEN_QUERIES_MS
cfg.LogDirectory             = RUN_LOCAL
cfg.RampSeconds              = USER_RAMP_TIME_SEC
cfg.LogFileName              = LOG_FILE
cfg.SkipResults              = SKIP_RESULTS

print(f"Run ID  : {RUN_ID}")
print(f"Endpoint: {xmla}")
print(f"Users   : {len(users)} concurrent, ramp {USER_RAMP_TIME_SEC}s, duration {DURATION_SECONDS}s")
print(f"Logs    : {RUN_LOCAL}/{LOG_FILE}")
""")

    # 5. Start run + handle polling loop
    code(nb, r"""
# ── 5. Run the load test ──────────────────────────────────────────────────────
# Press the ■ Interrupt Kernel button (or Esc, I-I) to cancel — the .NET
# threads drain cleanly via cooperative cancellation.
from IPython.display import display, update_display

handle = QueryRunner.StartLoadTest(cfg)
display({"text/plain": f"Starting run {handle.RunId} ..."},
        raw=True, display_id="fdlt-status")

def render(s):
    if s is None:
        return {"text/plain": "(initializing)"}
    line = (
        f"[{s.Phase:<10}] elapsed={s.Elapsed.TotalSeconds:6.1f}s  "
        f"users={s.ActiveUsers}/{s.TargetUsers}  "
        f"ok={s.Successful}  err={s.Failed}  "
        f"qps={s.RollingQps:.1f}"
    )
    return {"text/plain": line}

try:
    while not handle.IsCompleted:
        update_display(render(handle.LatestSnapshot), raw=True, display_id="fdlt-status")
        time.sleep(1)
except KeyboardInterrupt:
    print("Interrupt received — cancelling...")
    handle.Cancel()

result_json = handle.Wait()
update_display(render(handle.LatestSnapshot), raw=True, display_id="fdlt-status")

stats = json.loads(result_json)
print()
print(f"=== Results ===")
print(f"Phase            : {handle.LatestSnapshot.Phase}")
print(f"Total executions : {stats.get('totalExecutions')}")
print(f"Successful       : {stats.get('successfulExecutions')}")
print(f"Failed           : {stats.get('failedExecutions')}")
print(f"QPS              : {stats.get('qps')}")
lat = stats.get("latency", {})
if lat:
    print(f"Latency (ms)     : min={lat.get('min')}  median={lat.get('median')}  "
          f"mean={lat.get('mean')}  p95={lat.get('p95')}  p99={lat.get('p99')}  max={lat.get('max')}")

# Save full JSON next to the CSV.
with open(os.path.join(RUN_LOCAL, "result.json"), "w") as f:
    json.dump(stats, f, indent=2)

# Persist run artifacts to OneLake under Files/runs/<RUN_ID>/.
RUN_DEST = f"{LH_ABFSS}/Files/runs/{RUN_ID}"
notebookutils.fs.cp(f"file://{RUN_LOCAL}", RUN_DEST, recurse=True)
print(f"\nRun artifacts: {RUN_DEST}")
""")

    # 6. Charts
    code(nb, r"""
# ── 6. Charts ─────────────────────────────────────────────────────────────────
import pandas as pd, matplotlib.pyplot as plt

csv = os.path.join(RUN_LOCAL, LOG_FILE)
df  = pd.read_csv(csv)
print(f"Records: {len(df):,}  Success: {(df.Outcome=='Success').sum():,}  "
      f"Error: {(df.Outcome=='Error').sum():,}")

t_min, t_max = df.StartTimeMs.min(), df.StartTimeMs.max()
duration_s = max((t_max - t_min) / 1000, 1)
n_buckets  = min(100, max(1, len(df)))
df["bucket"] = pd.cut(df.StartTimeMs, bins=n_buckets, labels=False)

ok  = df[df.Outcome == "Success"]
err = df[df.Outcome == "Error"]
agg = ok.groupby("bucket").agg(
    count=("DurationMs", "count"), mean_ms=("DurationMs", "mean"),
    min_ms=("DurationMs", "min"),  max_ms=("DurationMs", "max"),
    t=("StartTimeMs", "mean"),
).reset_index()
errs  = err.groupby("bucket").agg(err_count=("DurationMs", "count")).reset_index()
users = df.groupby("bucket").agg(active_users=("ActiveUsers", "max")).reset_index()
agg = agg.merge(errs, on="bucket", how="left").merge(users, on="bucket", how="left").fillna(0)
agg["time_s"] = (agg.t - t_min) / 1000

fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 10), sharex=True,
                                    gridspec_kw={"height_ratios": [3, 1, 1]})
ax1.fill_between(agg.time_s, agg.min_ms, agg.max_ms, alpha=0.15, color="steelblue", label="min–max")
ax1.plot(agg.time_s, agg.mean_ms, color="steelblue", linewidth=1.5, label="mean")
ax1.plot(agg.time_s, agg.max_ms,  color="coral",     linewidth=0.8, alpha=0.7, label="max")
ax1.set_ylabel("Latency (ms)"); ax1.legend(loc="upper left"); ax1.grid(True, alpha=0.3)
ax1.set_title(f"Run {RUN_ID} — {len(df):,} queries / {duration_s:.0f}s / "
              f"{int(df.ActiveUsers.max())} concurrent users")

bw = duration_s / n_buckets if n_buckets > 0 else 1
ax2.bar(agg.time_s, agg["count"] / bw, width=bw * 0.9, color="steelblue", alpha=0.6, label="QPS (success)")
if agg.err_count.sum() > 0:
    ax2.bar(agg.time_s, agg.err_count / bw, width=bw * 0.9,
            bottom=agg["count"] / bw, color="red", alpha=0.6, label="QPS (error)")
ax2.set_ylabel("Queries/sec"); ax2.legend(loc="upper left"); ax2.grid(True, alpha=0.3)

ax3.plot(agg.time_s, agg.active_users, color="green", linewidth=1.5, label="Active users")
ax3.fill_between(agg.time_s, 0, agg.active_users, alpha=0.1, color="green")
ax3.set_ylabel("Users"); ax3.set_xlabel("Time (seconds)")
ax3.legend(loc="upper left"); ax3.grid(True, alpha=0.3); ax3.set_ylim(bottom=0)

plt.tight_layout(); plt.show()
""")

    write(nb, OUT / "Run.ipynb")


# ────────────────────────────────────────────────────────────────────────────
# Queries.ipynb — query catalog editor
# ────────────────────────────────────────────────────────────────────────────
def build_queries():
    nb = new_notebook()

    md(nb, r"""
# FabricDaxLoadTest — Queries

Manages the DAX query corpus stored at `Files/queries.json` in the
**`LoadTests`** lakehouse (same workspace folder as this notebook).

The `Run.ipynb` notebook reads this file by default. Inline overrides
on `Run.ipynb` (`QUERIES_INLINE`) take precedence when set.

`queries.json` is either:

- a JSON list of strings: `["EVALUATE ROW(\"x\", 1)", ...]`, or
- a JSON list of objects with a `query` key:
  `[{"query": "EVALUATE ...", "name": "...", "tags": [...]}]` — extra
  fields are preserved on round-trip but ignored by the runner.
""")

    code(nb, r"""
# ── 1. Locate the LoadTests lakehouse ─────────────────────────────────────────
import json, notebookutils
ctx = notebookutils.runtime.context
WS_ID = ctx["currentWorkspaceId"]
LH_ABFSS = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/LoadTests.Lakehouse"
QPATH = f"{LH_ABFSS}/Files/queries.json"
print(f"Lakehouse: {LH_ABFSS}")
print(f"Catalog  : {QPATH}")
""")

    code(nb, r"""
# ── 2. Read the current catalog ───────────────────────────────────────────────
try:
    raw = notebookutils.fs.head(QPATH, 1024 * 1024 * 4)
    queries = json.loads(raw)
    print(f"Loaded {len(queries)} queries from {QPATH}")
except Exception as e:
    print(f"(no existing catalog — will create on save: {e})")
    queries = []

import pandas as pd
def to_df(qs):
    rows = []
    for i, q in enumerate(qs):
        if isinstance(q, str):
            rows.append({"i": i, "name": "", "tags": "", "query": q})
        else:
            rows.append({
                "i":     i,
                "name":  q.get("name", ""),
                "tags":  ",".join(q.get("tags", [])) if isinstance(q.get("tags"), list) else (q.get("tags") or ""),
                "query": q.get("query", ""),
            })
    return pd.DataFrame(rows)

df = to_df(queries)
display(df.head(50))
""")

    code(nb, r"""
# ── 3. Edit the catalog ───────────────────────────────────────────────────────
# Edit the Python literal below and re-run cells 3 + 4. Each entry is a string
# (just the DAX) or a dict with optional name/tags. Strings work for simple
# cases; switch to dicts when you want labels in the report.
new_queries = [
    # Replace these examples with your real corpus.
    "EVALUATE ROW(\"x\", 1)",
    {"name": "topn-sales", "tags": ["sales", "topn"],
     "query": "EVALUATE TOPN(10, SUMMARIZECOLUMNS('Date'[Year], \"Sales\", [Sales Amount]))"},
]
print(f"Prepared {len(new_queries)} queries.")
display(to_df(new_queries))
""")

    code(nb, r"""
# ── 4. Save back to OneLake ───────────────────────────────────────────────────
notebookutils.fs.put(QPATH, json.dumps(new_queries, indent=2), overwrite=True)
print(f"Saved {len(new_queries)} queries to {QPATH}")
""")

    write(nb, OUT / "Queries.ipynb")


if __name__ == "__main__":
    build_run()
    build_queries()
