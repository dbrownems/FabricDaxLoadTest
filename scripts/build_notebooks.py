"""Generate notebooks/LoadTest-Template.ipynb.

The notebook is deployed into a `LoadTests` workspace folder by
scripts/Deploy-LoadTests.ps1, alongside a `LoadTests` lakehouse that
holds Files/loadgen-bin.zip (the LoadGen+ADOMD assemblies) and per-run
telemetry under Files/runs/.

The notebook self-discovers the workspace + lakehouse at run time via
`notebookutils.runtime.context`, so it is workspace-portable and does
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
    "kernelspec":    {"display_name": "Synapse PySpark", "name": "synapse_pyspark", "language": "Python"},
    "kernel_info":   {"name": "synapse_pyspark"},
    "language_info": {"name": "python"},
    "microsoft":     {"language": "python", "language_group": "synapse_pyspark"},
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
# LoadTest-Template.ipynb — the runner template
# ────────────────────────────────────────────────────────────────────────────
def build_run():
    nb = new_notebook()

    md(nb, r"""
# FabricDaxLoadTest — LoadTest Template

> ⚠️ **This is the template — do not edit or run this notebook in place.**
>
> 1. **File → Save As** (or right-click the notebook in the workspace → **Duplicate**).
> 2. Rename the copy to `LoadTest - <descriptive name>` (e.g.
>    `LoadTest - DIAD 5u baseline`). Keep it in the same `LoadTests` folder
>    so it can find `LoadTests.Lakehouse`.
> 3. Open the copy, edit cell **1**, and run.
>
> Why: `scripts/Deploy-LoadTests.ps1` overwrites this template on every
> redeploy. Saved copies are yours forever — they document the exact
> configuration of each run and stay reproducible across upgrades.

---

Drives concurrent DAX queries against a Power BI / Fabric semantic model via
the **XMLA endpoint** by launching `LoadGen.dll` as an out-of-process
subprocess (run on the Spark driver's bundled .NET 8 runtime).

The notebook auto-discovers the workspace's **`LoadTests`** lakehouse — no
UI attach step required. The lakehouse is the default storage for
everything:

- `Files/loadgen-bin.zip` — LoadGen + ADOMD assemblies; cell 2 downloads
  and unzips this into a `/tmp` staging dir on the Spark driver
- `Files/runs/`  — per-run telemetry CSVs (created on first run)
- `Tables/dbo/LoadTest{s,Runs,Queries,QueryExecutions}` — Delta tables fed by cell 5b

## How to use (in your saved copy)

1. **Set up the query corpus.** Cell 1 ships with a tiny `QUERIES_INLINE`
   fallback (3 model-agnostic warm-up queries) — *only* useful for smoke
   testing the pipeline. For a real test you have two options:

   - **Drag a `PowerBiPerformance.json` onto the notebook's *Resources*
     panel** (left sidebar). This is the canonical workflow — the corpus
     travels with the saved `LoadTest - <name>` notebook so each test is
     reproducible. (Power BI Desktop's *Performance Analyzer* exports
     query traces in this exact format; the runner accepts that file
     verbatim, plus simpler list-of-strings JSON.)
   - **Edit `QUERIES_INLINE` in cell 1** with the DAX you want to drive.
     Fine for one-off tests; doesn't scale to large corpora.

2. Edit cell **1** to point at the target workspace + dataset and tweak
   load parameters. Set `LOAD_TEST_NAME` / `LOAD_TEST_DESCRIPTION` —
   these land in the `LoadTests` dim and surface in the Power BI report.
3. **Run All**. Cell **4** prints a live status line every second; press
   **Interrupt Kernel** (■) to cancel — the subprocess receives SIGINT
   and drains cleanly.
4. Cell **5b** writes the run into the four Delta tables. Every
   notebook execution mints a fresh `RunId`, so prior runs are
   preserved untouched — re-running the notebook is purely additive.
   Re-executing **only cell 5b** (after a completed run) is also safe:
   it overwrites just that one `RunId`'s fact rows in place.
5. Cell **6** plots latency / QPS / users from the per-run CSV.

> Re-deploy / upgrade `Files/loadgen-bin.zip` by re-running
> `scripts/Deploy-LoadTests.ps1` from a clone of the repo. Your saved
> `LoadTest - …` notebooks are not touched by the deploy.
""")

    # 1. Configuration
    code(nb, r"""
# ── 1. Configuration ──────────────────────────────────────────────────────────
LOAD_TEST_NAME           = "my-load-test"  # human label written to LoadTests.Name
LOAD_TEST_DESCRIPTION    = ""              # optional free text

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

# A small, model-agnostic warm-up corpus used when no resource file is
# attached (see QUERIES_FILE below). Useful for smoke testing the
# pipeline; replace with real queries — either inline here or by
# uploading a `PowerBiPerformance.json` to this notebook's Resources
# panel — before drawing any performance conclusions.
QUERIES_INLINE = [
    "EVALUATE ROW(\"ping\", 1)",
    "EVALUATE INFO.TABLES()",
    "EVALUATE INFO.MEASURES()",
]

# Path to the DAX query corpus.
#
# Canonical workflow: drag a `PowerBiPerformance.json` file onto this
# notebook's *Resources* panel (left sidebar). Power BI Desktop's
# *Performance Analyzer* exports query traces in this exact format; the
# runner also accepts a plain JSON list of DAX strings. Cell 3 resolves
# `QUERIES_FILE` in this order:
#
#   1. notebook resources (`builtin/<QUERIES_FILE>`) — preferred; the
#      corpus travels with the saved `LoadTest - <name>` notebook.
#   2. literal `abfss://…` URL — escape hatch for cross-lakehouse refs.
#   3. fall back to `QUERIES_INLINE` above.
QUERIES_FILE = "PowerBiPerformance.json"

# Optional inline users; if empty, all virtual users share the interactive
# token's identity (no role / CustomData impersonation). Each entry is
# {"email": "...", "role": "..."} — the role string is forwarded to the AS
# `Roles=` connection string property.
USERS_INLINE = []
""")

    # 2. Auto-discover lakehouse + stage LoadGen + dotnet preflight
    code(nb, r"""
# ── 2. Download + unzip LoadGen and locate dotnet ────────────────────────────
# This is a Fabric **PySpark** notebook. The workspace contains exactly one
# `LoadTests` lakehouse (created by `scripts/Deploy-LoadTests.ps1` or the
# manual setup) — we discover it via the Fabric items API and use it as the
# default storage. The LoadGen assemblies live in a single zip at
# `Files/loadgen-bin.zip`; cell 2 downloads it once per kernel and unzips
# into `/tmp/fdlt-bin/`.
#
# We run LoadGen out-of-process (`dotnet LoadGen.dll`) to avoid the
# pythonnet / CLR-init footguns that come with sharing the Spark driver's
# CLR with sempy.
import os, json, shutil, subprocess, zipfile
import notebookutils

ctx = notebookutils.runtime.context
WS_ID   = ctx["currentWorkspaceId"]
WS_NAME = ctx.get("currentWorkspaceName", WS_ID)

# Self-check: this is a template — refuse to run unless saved under a new name.
_self_name = (ctx.get("currentNotebookName") or "").strip()
if _self_name == "LoadTest - Template":
    raise RuntimeError(
        "This is the LoadTest template. Save As → 'LoadTest - <name>' "
        "and run that copy. Edits to the template are wiped on every "
        "scripts/Deploy-LoadTests.ps1 redeploy."
    )

# Resolve LoadTests lakehouse — friendly-name support is disabled on some
# OneLake tenants, so we look up the GUID via the Fabric items API and use
# that in the abfss path. notebookutils Fabric audience is "pbi".
LAKEHOUSE_NAME = "LoadTests"
import requests
_tok = notebookutils.credentials.getToken("pbi")
_r = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/items?type=Lakehouse",
    headers={"Authorization": f"Bearer {_tok}"}, timeout=30)
_r.raise_for_status()
_match = [i for i in _r.json().get("value", []) if i["displayName"] == LAKEHOUSE_NAME]
if not _match:
    raise RuntimeError(f"Lakehouse '{LAKEHOUSE_NAME}' not found in workspace {WS_ID}")
LH_ID = _match[0]["id"]
LH_ABFSS = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}"

# Download Files/loadgen-bin.zip → local /tmp, then extract. Reuses the
# previous extraction across cell re-runs within a kernel session (cheap
# mtime check) but always re-downloads if missing.
STAGE = "/tmp/fdlt-bin"
ZIP_LOCAL = "/tmp/fdlt-loadgen-bin.zip"
ZIP_REMOTE = f"{LH_ABFSS}/Files/loadgen-bin.zip"
if os.path.exists(ZIP_LOCAL):
    os.remove(ZIP_LOCAL)
notebookutils.fs.cp(ZIP_REMOTE, f"file://{ZIP_LOCAL}")
shutil.rmtree(STAGE, ignore_errors=True)
os.makedirs(STAGE, exist_ok=True)
with zipfile.ZipFile(ZIP_LOCAL, "r") as zf:
    zf.extractall(STAGE)

LOADGEN_DLL = os.path.join(STAGE, "LoadGen.dll")
if not os.path.exists(LOADGEN_DLL):
    raise FileNotFoundError(
        f"LoadGen.dll not found under {STAGE} after extracting {ZIP_REMOTE}. "
        f"Re-run scripts/Deploy-LoadTests.ps1 from a repo clone to refresh "
        f"the lakehouse zip."
    )

# Locate dotnet. Fabric Spark drivers ship the .NET 8 runtime under sempy's
# trident_env. Prefer that over $PATH so version mismatches with whatever
# the user happens to have don't bite us.
_DOTNET_CANDIDATES = [
    os.environ.get("DOTNET_HOST_PATH"),
    "/home/trusted-service-user/cluster-env/trident_env/bin/dotnet",
    shutil.which("dotnet"),
    "/usr/bin/dotnet",
    "/usr/local/bin/dotnet",
]
DOTNET = next((p for p in _DOTNET_CANDIDATES if p and os.path.exists(p)), None)
if DOTNET is None:
    raise RuntimeError(
        "Could not find a `dotnet` runtime on this kernel. LoadGen.dll is a "
        "framework-dependent .NET 8 build and needs the runtime to be installed. "
        f"Probed: {[c for c in _DOTNET_CANDIDATES if c]}"
    )
# Sanity check the runtime can actually load — fail fast if the host is broken.
_info = subprocess.run([DOTNET, "--info"], capture_output=True, text=True, timeout=10)
if _info.returncode != 0:
    raise RuntimeError(f"`{DOTNET} --info` failed:\n{_info.stderr}")

print(f"Workspace : {WS_NAME} ({WS_ID})")
print(f"Lakehouse : {LAKEHOUSE_NAME} ({LH_ID})")
print(f"LoadGen   : {LOADGEN_DLL}  ({os.path.getsize(LOADGEN_DLL):,} bytes)")
print(f"dotnet    : {DOTNET}")
""")

    # 3. Build run config (queries, users, paths, token)
    code(nb, r"""
# ── 3. Build the run config and resolve token ────────────────────────────────
import time, uuid
from datetime import datetime, timezone

# Queries — try notebook resources first (canonical); else literal abfss://;
# else fall back to QUERIES_INLINE (the model-agnostic warm-up corpus).
def _load_queries():
    if QUERIES_FILE.startswith("abfss://"):
        raw = notebookutils.fs.head(QUERIES_FILE, 1024 * 1024 * 4)
        return [q if isinstance(q, str) else q["query"] for q in json.loads(raw)], QUERIES_FILE
    # Notebook resources are mounted at ./builtin/ in the kernel's CWD.
    res_path = f"builtin/{QUERIES_FILE.lstrip('/')}"
    if os.path.exists(res_path):
        with open(res_path, "r", encoding="utf-8") as f:
            raw = f.read()
        return [q if isinstance(q, str) else q["query"] for q in json.loads(raw)], f"resources:{res_path}"
    return list(QUERIES_INLINE), "(QUERIES_INLINE fallback)"

queries, queries_source = _load_queries()
print(f"Queries : {len(queries)}  from {queries_source}")
if queries_source.startswith("(QUERIES_INLINE"):
    print("          ⚠ no resource file attached — using model-agnostic warm-up queries only.")
    print("          Attach `PowerBiPerformance.json` to the notebook Resources panel for a real test.")

# Users — round-robin to CONCURRENT_USERS
if USERS_INLINE:
    base = list(USERS_INLINE)
else:
    base = [{"email": "anonymous@local", "role": ""}]
users = [base[i % len(base)] for i in range(CONCURRENT_USERS)]

# Run output dir under Files/runs/<runId>/ — LoadGen will write
# LoadTest.*.csv, LoadTest.*.log, and result.json in here.
# STAGING_ID is a short id used only for the local temp path; the canonical
# RunId is the GUID LoadGen embeds in every telemetry row, captured from the
# `started` envelope in cell 4.
STAGING_ID = uuid.uuid4().hex[:8]
RUN_ID     = None  # populated by cell 4 from the LoadGen `started` envelope
RUN_LOCAL  = f"/tmp/fdlt-run-{STAGING_ID}"
os.makedirs(RUN_LOCAL, exist_ok=True)
LOG_FILE   = f"LoadTest.{CONCURRENT_USERS}u.{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.csv"

# Token (Power BI XMLA audience — same `pbi` key as the Fabric REST call above).
TOKEN = notebookutils.credentials.getToken("pbi")

# Materialize queries.json + users.json next to the run dir. Passing them
# as files (instead of CLI flags) keeps multi-line DAX intact and avoids
# any quoting hazard on the subprocess command line.
QUERIES_JSON = os.path.join(RUN_LOCAL, "queries.json")
USERS_JSON   = os.path.join(RUN_LOCAL, "users.json")
with open(QUERIES_JSON, "w", encoding="utf-8") as f:
    json.dump(list(queries), f)
with open(USERS_JSON, "w", encoding="utf-8") as f:
    json.dump([{"email": u["email"], "role": u.get("role", "")} for u in users], f)

xmla = f"powerbi://api.powerbi.com/v1.0/myorg/{TARGET_WORKSPACE}"

print(f"Staging : {STAGING_ID} (RunId assigned by LoadGen)")
print(f"Endpoint: {xmla}{('?' + TARGET_REPLICA) if TARGET_REPLICA else ''}")
print(f"Users   : {len(users)} concurrent, ramp {USER_RAMP_TIME_SEC}s, duration {DURATION_SECONDS}s")
print(f"Logs    : {RUN_LOCAL}/{LOG_FILE}")
""")

    # 4. Launch LoadGen subprocess + stream JSONL progress
    code(nb, r"""
# ── 4. Run the load test (out-of-process) ────────────────────────────────────
# We launch `dotnet LoadGen.dll --json-progress ...` and read line-delimited
# JSON envelopes from its stdout. Stderr (banner, .NET log lines, exception
# dumps) is drained on a background thread and printed on failure.
#
# Press the ■ Interrupt Kernel button (or Esc, I-I) to cancel — we forward
# SIGINT to the child, which calls handle.Cancel() and drains cleanly.
import signal, threading, sys
from collections import deque
from IPython.display import display, update_display

cmd = [
    DOTNET, LOADGEN_DLL, "--json-progress",
    "--xmla", xmla,
    "--dataset", TARGET_DATASET,
    "--duration", str(DURATION_SECONDS),
    "--users", str(CONCURRENT_USERS),
    "--queries-per-batch", str(QUERIES_PER_BATCH),
    "--pause-iterations", str(PAUSE_BETWEEN_ITERATIONS_MS),
    "--pause-queries", str(PAUSE_BETWEEN_QUERIES_MS),
    "--ramp-time", str(USER_RAMP_TIME_SEC),
    "--queries-file", QUERIES_JSON,
    "--users-file", USERS_JSON,
    "--log-dir", RUN_LOCAL,
    "--log-file", LOG_FILE,
]
if TARGET_REPLICA:
    cmd += ["--replica", TARGET_REPLICA]
if SKIP_RESULTS:
    cmd += ["--skip-results"]

# Token via env, NOT argv: process listings on shared compute would otherwise
# expose the bearer token.
env = {**os.environ, "PBI_TOKEN": TOKEN}

display({"text/plain": f"Starting (staging={STAGING_ID}) ..."}, raw=True, display_id="fdlt-status")

proc = subprocess.Popen(
    cmd, env=env,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    text=True, bufsize=1,                # line-buffered text streams
)

# Drain stderr to a ring buffer so we can surface it on failure without
# blocking the parent on a full pipe (CPython's small default pipe buffer
# stalls the child if either stream is left unread).
stderr_buf = deque(maxlen=1000)
def _drain_stderr():
    for line in proc.stderr:
        stderr_buf.append(line.rstrip("\n"))
threading.Thread(target=_drain_stderr, daemon=True).start()

def _render(envelope):
    if envelope is None:
        return {"text/plain": "(initializing)"}
    if envelope.get("type") == "progress":
        return {"text/plain": (
            f"[{envelope.get('phase','?'):<10}] "
            f"elapsed={envelope.get('elapsed',0):6.1f}s  "
            f"users={envelope.get('activeUsers',0)}/{envelope.get('targetUsers',0)}  "
            f"ok={envelope.get('successful',0)}  err={envelope.get('failed',0)}  "
            f"qps={envelope.get('qps',0):.1f}"
        )}
    return {"text/plain": json.dumps(envelope)}

result_envelope = None
error_envelope  = None

try:
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            env_obj = json.loads(line)
        except json.JSONDecodeError:
            # Stray non-JSON output is unexpected in --json-progress mode but
            # not fatal — log it via the live status line so it's visible.
            update_display({"text/plain": f"(non-JSON stdout) {line}"},
                           raw=True, display_id="fdlt-status")
            continue
        kind = env_obj.get("type")
        if kind == "started" and env_obj.get("runId"):
            # Second `started` envelope (post-StartLoadTest) carries the
            # canonical RunId GUID that LoadGen wrote into the CSV. The
            # first `started` envelope (parameter banner) lacks it.
            RUN_ID = env_obj["runId"]
            update_display({"text/plain": f"Started run {RUN_ID}"},
                           raw=True, display_id="fdlt-status")
        elif kind == "progress":
            update_display(_render(env_obj), raw=True, display_id="fdlt-status")
        elif kind == "result":
            result_envelope = env_obj
        elif kind == "error":
            error_envelope = env_obj
        # else: ignore unknown types (forward-compat)
except KeyboardInterrupt:
    print("Interrupt received — sending SIGINT to LoadGen to drain...")
    try: proc.send_signal(signal.SIGINT)
    except Exception: pass
    # Give LoadGen up to 30s to drain; if it hangs, terminate.
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        print("LoadGen did not exit in 30s after SIGINT — terminating.")
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()
finally:
    proc.wait()

returncode = proc.returncode
""")

    # 5. Surface results / failure
    code(nb, r"""
# ── 5. Surface results (or failure detail) ──────────────────────────────────
def _print_log_tail(label="LoadGen .log"):
    try:
        import glob as _glob
        logs = sorted(_glob.glob(os.path.join(RUN_LOCAL, "*.log")))
        if logs:
            print(f"\n--- tail of {os.path.basename(logs[-1])} ({label}) ---")
            with open(logs[-1], "r", encoding="utf-8", errors="replace") as _lf:
                lines = _lf.readlines()
            for _line in lines[-100:]:
                print(_line.rstrip())
    except Exception as _le:
        print(f"(could not read log file: {_le})")

def _print_stderr_tail(n=40):
    if stderr_buf:
        print(f"\n--- LoadGen stderr (last {min(n, len(stderr_buf))} lines) ---")
        tail = list(stderr_buf)[-n:]
        for line in tail:
            print(line)

# Persist run artifacts to OneLake under Files/runs/<RunId or staging id>/.
RUN_DEST = f"{LH_ABFSS}/Files/runs/{RUN_ID or STAGING_ID}"
try:
    notebookutils.fs.cp(f"file://{RUN_LOCAL}", RUN_DEST, recurse=True)
except Exception as _cp_ex:
    print(f"(warning: failed to persist run artifacts to OneLake: {_cp_ex})")

if error_envelope is not None or returncode not in (0, 130):
    print()
    print("=== Load test FAILED ===")
    if error_envelope is not None:
        print(f"code   : {error_envelope.get('code')}")
        print(f"type   : {error_envelope.get('exceptionType')}")
        print(f"message:")
        for ml in str(error_envelope.get("message", "")).splitlines():
            print(f"  {ml}")
    print(f"exit code: {returncode}")
    _print_stderr_tail(40)
    _print_log_tail("on failure")
    print(f"\nRun artifacts (partial): {RUN_DEST}")
    raise RuntimeError(error_envelope.get("message", "LoadGen exited non-zero")
                       if error_envelope else f"LoadGen exited with code {returncode}")

if returncode == 130:
    print("\n=== Load test CANCELLED ===")
    if result_envelope is None:
        # Cancelled before completion finalized stats.
        _print_stderr_tail(20)
        _print_log_tail("on cancel")
        print(f"\nRun artifacts: {RUN_DEST}")
        # Treat cancel as a clean exit from the notebook flow; no raise.
    # else: fall through and print whatever stats we got.

if result_envelope is not None:
    summary = result_envelope.get("summary", {}) or {}
    print()
    print("=== Results ===")
    print(f"Total executions : {summary.get('totalExecutions')}")
    print(f"Successful       : {summary.get('successfulExecutions')}")
    print(f"Failed           : {summary.get('failedExecutions')}")
    print(f"QPS              : {summary.get('qps')}")
    lat = summary.get("latency", {}) or {}
    if lat:
        print(f"Latency (ms)     : min={lat.get('min')}  median={lat.get('median')}  "
              f"mean={lat.get('mean')}  p95={lat.get('p95')}  p99={lat.get('p99')}  max={lat.get('max')}")
    print(f"\nFull result      : {result_envelope.get('resultFile')}")
    print(f"Run artifacts    : {RUN_DEST}")
""")

    # 5b. Persist run to Lakehouse Delta tables (§1.6 unified-trace ready)
    code(nb, r"""
# ── 5b. Persist run to Lakehouse Delta tables ────────────────────────────────
# Writes 4 tables into the host Lakehouse:
#   LoadTests                 — 1 row per logical test (MERGE on LoadTestId)
#   LoadTestRuns              — 1 row per run (MERGE on RunId; OwnerType-keyed for §1.6)
#   LoadTestQueries           — 1 row per (LoadTestId, QueryIndex) (insert-only)
#   LoadTestQueryExecutions   — 1 row per query attempt. Idempotent only
#       for re-runs of cell 5b alone (DELETE WHERE RunId / INSERT). Each
#       full notebook execution mints a fresh RunId, so prior runs are
#       preserved untouched and re-running the notebook is purely additive.
#
# TraceEvents / QueryCompleted / SecondBuckets tables are intentionally NOT
# written here — those require Phase-1 trace capture, which is a separate
# milestone. The schema is forward-compatible with §1.6: facts that will join
# to trace events carry OwnerType/OwnerId/OwnerKey columns so the eventual
# TraceOwners-driven slicer works without a migration.
import hashlib
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType,
)
from delta.tables import DeltaTable

# --- discover identifiers --------------------------------------------------
# LoadTestId == the Fabric notebook item GUID per §1.6 §Table 1. Fall back
# to a deterministic UUID derived from (workspace, notebook name) if the
# runtime doesn't expose the notebook id (e.g. running inline from a Livy
# session that isn't backed by a saved notebook).
import uuid as _uuid
_notebook_id = ctx.get("currentNotebookId") or ctx.get("notebookId")
if _notebook_id:
    LOAD_TEST_ID = str(_notebook_id)
else:
    LOAD_TEST_ID = str(_uuid.uuid5(_uuid.NAMESPACE_URL,
        f"fdlt://{WS_ID}/{LOAD_TEST_NAME}"))
_notebook_name = ctx.get("currentNotebookName") or LOAD_TEST_NAME

# --- corpus hash + per-query hashes ----------------------------------------
def _hash_query(text: str) -> str:
    return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()

query_hashes = [_hash_query(q) for q in queries]
corpus_hash  = hashlib.sha256(
    ("\u0001".join(query_hashes)).encode("utf-8")
).hexdigest()

# --- run-level rollups from the result envelope ----------------------------
_summary = (result_envelope or {}).get("summary", {}) if result_envelope else {}
_lat     = _summary.get("latency", {}) or {}
_run_status = "Aborted" if (error_envelope is not None) else (
    "Cancelled" if returncode == 130 else "Completed")
_abort_reason = (error_envelope or {}).get("message", "") if error_envelope else ""

started_at = datetime.now(timezone.utc)  # approximate; real start was earlier
# Prefer the run's actual UTC start from the first CSV row below.

# --- read the per-query CSV ------------------------------------------------
import pandas as _pd
csv_path = os.path.join(RUN_LOCAL, LOG_FILE)
_df = _pd.read_csv(csv_path)
if len(_df) > 0:
    started_at = _pd.to_datetime(_df["StartUtc"].min(), utc=True).to_pydatetime()
    ended_at   = _pd.to_datetime(_df["EndUtc"].max(),   utc=True).to_pydatetime()
else:
    ended_at = started_at

# --- write the lakehouse tables --------------------------------------------
LH_TABLE_BASE = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Tables"

def _table_path(name): return f"{LH_TABLE_BASE}/{name}"

def _upsert(df, name, merge_keys):
    path = _table_path(name)
    if DeltaTable.isDeltaTable(spark, path):
        tgt = DeltaTable.forPath(spark, path)
        on = " AND ".join(f"t.{k}=s.{k}" for k in merge_keys)
        (tgt.alias("t")
            .merge(df.alias("s"), on)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        df.write.format("delta").mode("overwrite").save(path)

def _replace_for_run(df, name, run_id):
    path = _table_path(name)
    if DeltaTable.isDeltaTable(spark, path):
        DeltaTable.forPath(spark, path).delete(f"RunId = '{run_id}'")
        df.write.format("delta").mode("append")\
            .option("mergeSchema", "true").save(path)
    else:
        df.write.format("delta").mode("overwrite")\
            .option("mergeSchema", "true").save(path)

# LoadTests --------------------------------------------------------------
load_tests_df = spark.createDataFrame([Row(
    LoadTestId        = LOAD_TEST_ID,
    Name              = LOAD_TEST_NAME,
    Description       = LOAD_TEST_DESCRIPTION,
    WorkspaceId       = WS_ID,
    WorkspaceName     = WS_NAME,
    NotebookId        = LOAD_TEST_ID,
    NotebookName      = _notebook_name,
    TargetWorkspace   = TARGET_WORKSPACE,
    TargetDataset     = TARGET_DATASET,
    SourceType        = "HandAuthored",
    QueryCount        = len(queries),
    QueryCorpusHash   = corpus_hash,
    LastRunAtUtc      = started_at,
    LastRunId         = RUN_ID,
    Status            = "Active",
)])
_upsert(load_tests_df, "LoadTests", ["LoadTestId"])

# LoadTestRuns (carries OwnerType/OwnerId/OwnerKey per §1.6) -------------
runs_df = spark.createDataFrame([Row(
    RunId            = RUN_ID,
    LoadTestId       = LOAD_TEST_ID,
    RunName          = LOAD_TEST_NAME,
    OwnerType        = "LoadTestRun",
    OwnerId          = RUN_ID,
    OwnerKey         = f"LoadTestRun/{RUN_ID}",
    QueryCorpusHash  = corpus_hash,
    StartedAtUtc     = started_at,
    EndedAtUtc       = ended_at,
    WorkspaceName    = TARGET_WORKSPACE,
    DatasetName      = TARGET_DATASET,
    XmlaEndpoint     = xmla,
    Replica          = TARGET_REPLICA or "",
    UserCount        = int(CONCURRENT_USERS),
    DurationSec      = int(DURATION_SECONDS),
    RampSec          = int(USER_RAMP_TIME_SEC),
    QueriesPerBatch  = int(QUERIES_PER_BATCH),
    PauseIterMs      = int(PAUSE_BETWEEN_ITERATIONS_MS),
    PauseQueryMs     = int(PAUSE_BETWEEN_QUERIES_MS),
    SkipResults      = bool(SKIP_RESULTS),
    TotalQueries     = int(_summary.get("totalExecutions")     or len(_df)),
    SuccessfulQueries= int(_summary.get("successfulExecutions") or int((_df["Outcome"]=="Success").sum()) if len(_df) else 0),
    FailedQueries    = int(_summary.get("failedExecutions")    or int((_df["Outcome"]=="Error").sum())   if len(_df) else 0),
    Qps              = float(_summary.get("qps") or 0.0),
    Status           = _run_status,
    AbortReason      = _abort_reason,
    P50Ms            = float(_lat.get("median") or 0.0),
    P95Ms            = float(_lat.get("p95")    or 0.0),
    P99Ms            = float(_lat.get("p99")    or 0.0),
    MeanMs           = float(_lat.get("mean")   or 0.0),
)])
_upsert(runs_df, "LoadTestRuns", ["RunId"])

# LoadTestQueries (insert-only per LoadTestId/QueryIndex pair) -----------
queries_rows = [Row(
    LoadTestId = LOAD_TEST_ID,
    QueryIndex = i,
    QueryHash  = query_hashes[i],
    QueryText  = queries[i],
    SourceType = "HandAuthored",
) for i in range(len(queries))]
if queries_rows:
    queries_df = spark.createDataFrame(queries_rows)
    _upsert(queries_df, "LoadTestQueries", ["LoadTestId", "QueryIndex"])

# LoadTestQueryExecutions (the big fact) ---------------------------------
if len(_df) > 0:
    _df2 = _df.copy()
    _df2["StartUtc"] = _pd.to_datetime(_df2["StartUtc"], utc=True)
    _df2["EndUtc"]   = _pd.to_datetime(_df2["EndUtc"],   utc=True)
    # join QueryHash from the local list (notebook is authoritative)
    _df2["QueryHash"] = _df2["QueryIndex"].apply(
        lambda i: query_hashes[int(i)] if 0 <= int(i) < len(query_hashes) else None)
    exec_schema = StructType([
        StructField("RunId",              StringType(),    False),
        StructField("LoadTestId",         StringType(),    False),
        StructField("UserIndex",          IntegerType(),   False),
        StructField("UserEmail",          StringType(),    True),
        StructField("QueryIndex",         IntegerType(),   False),
        StructField("QueryHash",          StringType(),    True),
        StructField("Iteration",          IntegerType(),   False),
        StructField("StartUtc",           TimestampType(), False),
        StructField("EndUtc",             TimestampType(), True),
        StructField("StartTimeMs",        DoubleType(),    True),
        StructField("ClientDurationMs",   DoubleType(),    True),
        StructField("Outcome",            StringType(),    False),
        StructField("RowCount",           IntegerType(),   True),
        StructField("ResponseBytes",      LongType(),      True),
        StructField("ErrorMessage",       StringType(),    True),
        StructField("ActiveUsersAtStart", IntegerType(),   True),
    ])
    rows = [Row(
        RunId              = str(r["RunId"]),
        LoadTestId         = LOAD_TEST_ID,
        UserIndex          = int(r["UserIndex"]),
        UserEmail          = str(r["UserEmail"]) if _pd.notna(r["UserEmail"]) else None,
        QueryIndex         = int(r["QueryIndex"]),
        QueryHash          = r["QueryHash"],
        Iteration          = int(r["Iteration"]),
        StartUtc           = r["StartUtc"].to_pydatetime(),
        EndUtc             = r["EndUtc"].to_pydatetime() if _pd.notna(r["EndUtc"]) else None,
        StartTimeMs        = float(r["StartTimeMs"]) if _pd.notna(r["StartTimeMs"]) else None,
        ClientDurationMs   = float(r["DurationMs"])  if _pd.notna(r["DurationMs"]) else None,
        Outcome            = str(r["Outcome"]),
        RowCount           = int(r["RowCount"])      if _pd.notna(r["RowCount"]) else None,
        ResponseBytes      = int(r["ResponseBytes"]) if _pd.notna(r["ResponseBytes"]) else None,
        ErrorMessage       = (str(r["ErrorMessage"]) if _pd.notna(r["ErrorMessage"]) and str(r["ErrorMessage"]) else None),
        ActiveUsersAtStart = int(r["ActiveUsersAtStart"]) if _pd.notna(r["ActiveUsersAtStart"]) else None,
    ) for _, r in _df2.iterrows()]
    exec_df = spark.createDataFrame(rows, schema=exec_schema)
    _replace_for_run(exec_df, "LoadTestQueryExecutions", RUN_ID)

print(f"\n=== Lakehouse write OK ===")
print(f"  LoadTestId : {LOAD_TEST_ID}")
print(f"  RunId      : {RUN_ID}")
print(f"  Queries    : {len(queries)} (corpus hash {corpus_hash[:12]}...)")
print(f"  Executions : {len(_df):,}")
print(f"  Tables     : LoadTests, LoadTestRuns, LoadTestQueries, LoadTestQueryExecutions")
print(f"  Lakehouse  : {LAKEHOUSE_NAME} ({LH_ID})")
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
users = df.groupby("bucket").agg(active_users=("ActiveUsersAtStart", "max")).reset_index()
agg = agg.merge(errs, on="bucket", how="left").merge(users, on="bucket", how="left").fillna(0)
agg["time_s"] = (agg.t - t_min) / 1000

fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 10), sharex=True,
                                    gridspec_kw={"height_ratios": [3, 1, 1]})
ax1.fill_between(agg.time_s, agg.min_ms, agg.max_ms, alpha=0.15, color="steelblue", label="min–max")
ax1.plot(agg.time_s, agg.mean_ms, color="steelblue", linewidth=1.5, label="mean")
ax1.plot(agg.time_s, agg.max_ms,  color="coral",     linewidth=0.8, alpha=0.7, label="max")
ax1.set_ylabel("Latency (ms)"); ax1.legend(loc="upper left"); ax1.grid(True, alpha=0.3)
ax1.set_title(f"Run {RUN_ID} — {len(df):,} queries / {duration_s:.0f}s / "
              f"{int(df.ActiveUsersAtStart.max())} concurrent users")

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

    write(nb, OUT / "LoadTest-Template.ipynb")


if __name__ == "__main__":
    build_run()
