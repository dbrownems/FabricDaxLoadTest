"""Build notebooks/FabricDaxLoadTest.ipynb. Run once; commit the .ipynb output."""
import json, nbformat
from pathlib import Path

NB = nbformat.v4.new_notebook()
NB.metadata = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python"},
}

def md(text): NB.cells.append(nbformat.v4.new_markdown_cell(text.strip("\n")))
def code(text): NB.cells.append(nbformat.v4.new_code_cell(text.strip("\n")))

md(r"""
# Fabric DAX Load Test

Simulates concurrent users running DAX queries against a Power BI / Fabric semantic model
via the **XMLA endpoint** (ADOMD.NET), driven from this notebook through pythonnet.

This is a notebook-only replacement for
[FabricLoadTestTool](https://github.com/microsoft/fabric-toolbox/tree/main/tools/FabricLoadTestTool)
that gives you real per-user XMLA connections instead of REST.

## Prerequisites

1. A Lakehouse attached to this notebook (any will do — used for the DLL and CSV logs).
2. The semantic model is in a workspace your user account can access.
3. (Optional, for RLS testing) The model has roles using `CUSTOMDATA()` / `USERPRINCIPALNAME()`,
   and your account has **Build** + permission to test as role.

## How it works

1. Cell **1** — set test parameters.
2. Cell **2** — download `QueryRunner.dll` from the latest GitHub release into the lakehouse.
3. Cell **3** — load DAX queries + simulated users, acquire a Power BI bearer token.
4. Cell **4** — run the load test (this is the long cell, runs for `DURATION_SECONDS`).
5. Cell **5** — read the per-query telemetry CSV and render charts.
""")

code(r"""
# ── 1. Configuration ──────────────────────────────────────────────────────────
WORKSPACE      = "MyWorkspace"        # workspace name (the part after /myorg/ in the XMLA URL)
DATASET        = "My Semantic Model"  # semantic model display name

DURATION_SECONDS         = 60   # total test duration
CONCURRENT_USERS         = 50   # number of simulated concurrent users
QUERIES_PER_BATCH        = 1    # concurrent queries per user (>1 stresses engine concurrency)
PAUSE_BETWEEN_ITERATIONS_MS = 1000  # think-time between iterations of the query list
PAUSE_BETWEEN_QUERIES_MS    = 0     # think-time between individual queries (0 = none)
USER_RAMP_TIME_SEC       = 30   # ramp users 0 → CONCURRENT_USERS over this many seconds
TARGET_REPLICA           = ""   # "readonly" → scale-out read replica; "" → default
SKIP_RESULTS             = False  # True drains rows without parsing (cheaper client-side)

# DAX queries to cycle through. Each user runs the entire list per "iteration".
QUERIES = [
    "EVALUATE ROW(\"x\", 1)",
    # "EVALUATE TOPN(10, SUMMARIZECOLUMNS('Date'[Year], \"Sales\", [Sales Amount]))",
]

# Simulated users for RLS impersonation (CustomData / Roles).
# The interactive token holder must have permission to test as role.
USERS = [
    {"email": "user1@contoso.com", "role": ""},
    {"email": "user2@contoso.com", "role": ""},
    {"email": "user3@contoso.com", "role": ""},
]

# Pin to a specific QueryRunner.dll release tag, or "latest" for the most recent.
QUERY_RUNNER_RELEASE = "latest"
QUERY_RUNNER_REPO    = "dbrownems/FabricDaxLoadTest"

# Working directory in the attached lakehouse — DLL and CSV logs land here.
WORK_DIR = "/lakehouse/default/Files/loadtest"
""")

code(r"""
# ── 2. Download QueryRunner.dll from GitHub release ───────────────────────────
import os, json, urllib.request, glob

os.makedirs(WORK_DIR, exist_ok=True)

if QUERY_RUNNER_RELEASE == "latest":
    api_url = f"https://api.github.com/repos/{QUERY_RUNNER_REPO}/releases/latest"
else:
    api_url = f"https://api.github.com/repos/{QUERY_RUNNER_REPO}/releases/tags/{QUERY_RUNNER_RELEASE}"

with urllib.request.urlopen(api_url) as resp:
    release = json.loads(resp.read())

tag = release["tag_name"]
print(f"Release: {tag}")

# The release zip contains QueryRunner.dll plus AdomdClient and friends.
zip_asset = next((a for a in release["assets"] if a["name"].endswith(".zip")), None)
if zip_asset is None:
    raise RuntimeError("No .zip asset found in release. Build the project locally and copy DLLs to WORK_DIR instead.")

zip_local = os.path.join(WORK_DIR, zip_asset["name"])
if not os.path.exists(zip_local):
    urllib.request.urlretrieve(zip_asset["browser_download_url"], zip_local)
    print(f"Downloaded {zip_asset['name']}")

# Extract — use the cached zip if present.
import zipfile
with zipfile.ZipFile(zip_local) as zf:
    zf.extractall(WORK_DIR)

dll_path = os.path.join(WORK_DIR, "QueryRunner.dll")
if not os.path.exists(dll_path):
    raise FileNotFoundError(f"QueryRunner.dll missing after extract. Contents of {WORK_DIR}: {os.listdir(WORK_DIR)}")
print(f"QueryRunner.dll: {os.path.getsize(dll_path):,} bytes")
""")

code(r"""
# ── 3. Acquire token, expand users ────────────────────────────────────────────
import notebookutils

token = notebookutils.credentials.getToken("pbi")
print(f"Token acquired ({len(token)} chars)")

# Round-robin reuse if CONCURRENT_USERS > len(USERS).
users = [USERS[i % len(USERS)] for i in range(CONCURRENT_USERS)]
print(f"Queries: {len(QUERIES)}")
print(f"Users:   {len(users)} (from {len(USERS)} unique)")
print(f"Duration: {DURATION_SECONDS}s, ramp: {USER_RAMP_TIME_SEC}s")
""")

code(r"""
# ── 4. Run the load test ──────────────────────────────────────────────────────
from pythonnet import load
load("coreclr")
import clr, os, json, time
from datetime import datetime, timezone

# Bootstrap ADOMD.NET via sempy (it knows how to find the assemblies on the cluster).
import sempy.fabric as fabric
fabric.create_tom_server()

clr.AddReference(os.path.join(WORK_DIR, "QueryRunner.dll"))
from FabricDaxLoadTest import QueryRunner
from System import Array, String

import System.Reflection
asm = System.Reflection.Assembly.GetAssembly(QueryRunner)
print(f"QueryRunner v{asm.GetName().Version}")

# Build XMLA endpoint URL. ?readonly hits the scale-out read replica.
xmla_base = f"powerbi://api.powerbi.com/v1.0/myorg/{WORKSPACE}"
xmla = f"{xmla_base}?{TARGET_REPLICA}" if TARGET_REPLICA else xmla_base

q_arr     = Array[String]([q if isinstance(q, str) else q["query"] for q in QUERIES])
email_arr = Array[String]([u["email"] for u in users])
role_arr  = Array[String]([u["role"]  for u in users])

LOG_DIR = f"{WORK_DIR}/logs"
os.makedirs(LOG_DIR, exist_ok=True)

test_start_utc = datetime.now(timezone.utc)
log_file_name = f"LoadTest.{CONCURRENT_USERS}users.{test_start_utc.strftime('%Y%m%d-%H%M%S')}.csv"
print(f"Starting load test: {len(users)} users × {len(QUERIES)} queries, {DURATION_SECONDS}s")
print(f"Endpoint: {xmla}")
print(f"Log file: {log_file_name}")
print(flush=True)

t0 = time.time()
result_json = QueryRunner.RunLoadTest(
    q_arr, xmla, DATASET, token,
    email_arr, role_arr,
    DURATION_SECONDS,
    QUERIES_PER_BATCH,
    PAUSE_BETWEEN_ITERATIONS_MS,
    PAUSE_BETWEEN_QUERIES_MS,
    LOG_DIR,
    USER_RAMP_TIME_SEC,
    log_file_name,
    SKIP_RESULTS,
)
elapsed = time.time() - t0
test_end_utc = datetime.now(timezone.utc)

stats = json.loads(result_json)
print(f"\n=== Results ({elapsed:.0f}s wall-clock) ===")
print(f"Total executions: {stats['totalExecutions']}")
print(f"Successful:       {stats['successfulExecutions']}")
print(f"Failed:           {stats['failedExecutions']}")
print(f"QPS:              {stats['qps']}")

if "latency" in stats:
    lat = stats["latency"]
    print(f"\nLatency (ms):  min={lat['min']}  median={lat['median']}  mean={lat['mean']}  p95={lat['p95']}  p99={lat['p99']}  max={lat['max']}")

print(f"\nPer-user (first 10):")
for u in stats.get("perUser", [])[:10]:
    user = users[u["userIndex"]]
    print(f"  {user['email'][:30]:30s} iters={u['iterations']:<4} execs={u['executions']:<5} errs={u['errors']:<3} avg={u['meanLatencyMs']}ms")

if "sampleErrors" in stats:
    print(f"\nSample errors:")
    for e in stats["sampleErrors"][:5]:
        print(f"  User {e['UserIndex']}, Q{e['QueryIndex']}: {str(e['Error'])[:120]}")

with open(f"{WORK_DIR}/load_test_results.json", "w") as f:
    json.dump(stats, f, indent=2)
print(f"\nFull results: {WORK_DIR}/load_test_results.json")
print(f"Telemetry log: {stats.get('logFile', '?')}")
""")

code(r"""
# ── 5. Charts: latency, throughput, active users ─────────────────────────────
import pandas as pd
import matplotlib.pyplot as plt
import glob, os

log_files = sorted(glob.glob(f"{LOG_DIR}/LoadTest.*.csv"))
if not log_files:
    raise FileNotFoundError(f"No log files in {LOG_DIR}")
log_path = log_files[-1]
print(f"Reading: {os.path.basename(log_path)}")
df = pd.read_csv(log_path)
print(f"Records: {len(df):,}  Success: {(df.Outcome=='Success').sum():,}  Error: {(df.Outcome=='Error').sum():,}")

t_min, t_max = df.StartTimeMs.min(), df.StartTimeMs.max()
duration_s = (t_max - t_min) / 1000
n_buckets = min(100, max(1, len(df)))
df["bucket"] = pd.cut(df.StartTimeMs, bins=n_buckets, labels=False)

ok  = df[df.Outcome == "Success"]
err = df[df.Outcome == "Error"]

agg = ok.groupby("bucket").agg(
    count   = ("DurationMs", "count"),
    mean_ms = ("DurationMs", "mean"),
    min_ms  = ("DurationMs", "min"),
    max_ms  = ("DurationMs", "max"),
    t       = ("StartTimeMs", "mean"),
).reset_index()
err_agg  = err.groupby("bucket").agg(err_count=("DurationMs", "count")).reset_index()
user_agg = df.groupby("bucket").agg(active_users=("ActiveUsers", "max")).reset_index()
agg = agg.merge(err_agg, on="bucket", how="left").merge(user_agg, on="bucket", how="left").fillna(0)
agg["time_s"] = (agg.t - t_min) / 1000

fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 10), sharex=True,
                                    gridspec_kw={"height_ratios": [3, 1, 1]})

ax1.fill_between(agg.time_s, agg.min_ms, agg.max_ms, alpha=0.15, color="steelblue", label="min–max")
ax1.plot(agg.time_s, agg.mean_ms, color="steelblue", linewidth=1.5, label="mean")
ax1.plot(agg.time_s, agg.max_ms,  color="coral",     linewidth=0.8, alpha=0.7, label="max")
ax1.set_ylabel("Latency (ms)")
ax1.legend(loc="upper left")
ax1.set_title(f"Load Test — {len(df):,} queries over {duration_s:.0f}s, up to {int(df.ActiveUsers.max())} concurrent users")
ax1.grid(True, alpha=0.3)

bucket_width = duration_s / n_buckets if n_buckets > 0 else 1
ax2.bar(agg.time_s, agg["count"] / bucket_width, width=bucket_width * 0.9,
        color="steelblue", alpha=0.6, label="QPS (success)")
if agg.err_count.sum() > 0:
    ax2.bar(agg.time_s, agg.err_count / bucket_width, width=bucket_width * 0.9,
            bottom=agg["count"] / bucket_width, color="red", alpha=0.6, label="QPS (error)")
ax2.set_ylabel("Queries/sec")
ax2.legend(loc="upper left")
ax2.grid(True, alpha=0.3)

ax3.plot(agg.time_s, agg.active_users, color="green", linewidth=1.5, label="Active users")
ax3.fill_between(agg.time_s, 0, agg.active_users, alpha=0.1, color="green")
ax3.set_ylabel("Users")
ax3.set_xlabel("Time (seconds)")
ax3.legend(loc="upper left")
ax3.grid(True, alpha=0.3)
ax3.set_ylim(bottom=0)

plt.tight_layout()
plt.show()

print(f"\nDuration: {duration_s:.1f}s")
print(f"QPS:      {(df.Outcome=='Success').sum() / max(duration_s, 1):.1f}")
print(f"Users:    {df.UserEmail.nunique()} distinct")
if len(ok) > 0:
    print(f"Latency — min: {ok.DurationMs.min():.0f}ms  median: {ok.DurationMs.median():.0f}ms  "
          f"mean: {ok.DurationMs.mean():.0f}ms  p95: {ok.DurationMs.quantile(0.95):.0f}ms  "
          f"max: {ok.DurationMs.max():.0f}ms")
""")

# Patch all code cells with required GitHub-renderer fields
for c in NB.cells:
    if c.cell_type == "code":
        c["execution_count"] = None
        c["outputs"] = []
        c["metadata"] = {}

out = Path("C:/Users/david/source/repos/FabricDaxLoadTest/notebooks/FabricDaxLoadTest.ipynb")
out.parent.mkdir(parents=True, exist_ok=True)
with open(out, "w", encoding="utf-8") as f:
    nbformat.write(NB, f)

# Validate against GitHub's strict renderer
nbformat.validate(nbformat.read(str(out), as_version=4))
print(f"OK: {out}  ({out.stat().st_size:,} bytes, {len(NB.cells)} cells)")
