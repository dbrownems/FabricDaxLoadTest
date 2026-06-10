"""Generate notebooks/LoadTest-Main.ipynb.

The notebook is deployed into a `LoadTests` workspace folder by
scripts/Deploy-LoadTests.ps1, alongside a `LoadTests` lakehouse that
holds the five Delta tables under Tables[/dbo]/. Per-run telemetry
(CSVs, *.trace.csv, result.json, *.log) lives only on the Spark
driver's local /tmp for the lifetime of the kernel — it's strictly
forensic and never copied to OneLake.

As of v0.5.0 the .NET LoadGen binaries ship inside the `fdlt_runtime`
wheel, so cell 2 is a single `pip install <wheel>` — no zip download,
no unzip step. The default WHEEL_URL is replaced at release-build
time by setting the `FDLT_RELEASE_VERSION` env var (the GitHub
Release workflow sets it to the tag, e.g. `0.5.0`). For local
`Deploy-LoadTests.ps1` runs, the script patches WHEEL_URL inside
the generated notebook to point at the freshly-built wheel uploaded
to the lakehouse Files folder via abfss://.

The notebook self-discovers the workspace + lakehouse at run time via
`notebookutils.runtime.context`, so it is workspace-portable and does
not need rewriting per deployment.

Run from repo root:
    python scripts\\build_notebooks.py
"""
import json
import os
import nbformat
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT  = REPO / "notebooks"
OUT.mkdir(exist_ok=True, parents=True)

# Substituted into cell 2's WHEEL_URL string. The release workflow
# sets FDLT_RELEASE_VERSION to the tag (without the leading `v`); the
# deploy script leaves it unset and patches the resulting notebook
# in-place with an abfss:// URL after lakehouse resolution.
RELEASE_VERSION = os.environ.get("FDLT_RELEASE_VERSION", "").strip()
if RELEASE_VERSION:
    WHEEL_URL_DEFAULT = (
        f"https://github.com/dbrownems/FabricDaxLoadTest/releases/download/"
        f"v{RELEASE_VERSION}/fdlt_runtime-{RELEASE_VERSION}-py3-none-any.whl"
    )
else:
    # Sentinel — cell 2 raises if this is not overridden. Catches the
    # "ran build_notebooks.py with no env var, never patched it"
    # mistake at notebook runtime instead of silently 404-ing on pip
    # install.
    WHEEL_URL_DEFAULT = "REPLACE_ME_WITH_WHEEL_URL"

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
# LoadTest-Main.ipynb — the runner notebook
# ────────────────────────────────────────────────────────────────────────────
def build_run():
    nb = new_notebook()

    md(nb, r"""
    # FabricDaxLoadTest — LoadTest Main

    **This is your Load Test.** Edit cell 1 and run it. Each Run-All mints
    a fresh `RunId` so re-running is purely additive.

    ## Quickstart (4 steps, no lakehouse required)

    1. **Drop a queries `.json` onto the Resources panel** (left sidebar).
       Power BI Desktop *Performance Analyzer* exports work verbatim.
    2. **Edit cell 1**: set `TARGET_DATASET` (or leave `None` if there's
       only one semantic model in the workspace).
    3. **Run All.** Cell 3 prints a live status line; cell 4 plots
       latency / QPS / users / engine CPU.
    4. *(Optional)* set `LAKEHOUSE_NAME` in cell 1 to persist results to
       Delta tables for cross-run analysis.

    ### What's the lakehouse for?

    Charts in cell 4 read the LoadGen CSV directly from the Spark driver's
    local `/tmp/` — they need **no Spark and no lakehouse**. Setting
    `LAKEHOUSE_NAME` (cell 1) opts in to writing 5 Delta tables —
    `LoadTests`, `LoadTestRuns`, `QueryExecutions`,
    `TraceEvents` — keyed so multiple runs land side-by-side and can be
    queried as a Direct Lake source for dashboards. Without it, the
    forensic artifacts (CSVs, `*.log`, `*.trace.csv`) live only on the
    driver and disappear at session end.

    > **Detailed parameter reference.** Cell 1 ships with one-line comments;
    > the full explanation of every knob (semantics, defaults, examples) is
    > in [`docs/loadgen-main.md`](../docs/loadgen-main.md).

    > **Multiple Load Tests in one workspace?** **File → Save As** /
    > **Duplicate** and rename the copy to `LoadTest - <name>`.
    > `Deploy-LoadTests.ps1` only updates the original `LoadTest - Main`,
    > so cell-1 edits on saved copies survive redeploys.

    > **Upgrades.** Change `WHEEL_URL` in cell 2 to a newer release
    > (e.g. `v0.9.0` → `v0.10.0`) and Run All. The .NET LoadGen binaries
    > ship inside the `fdlt_runtime` wheel, so there is nothing else to
    > refresh.

    ---

    Drives concurrent DAX queries against a Power BI / Fabric semantic
    model via the **XMLA endpoint** by launching `LoadGen.dll` as an
    out-of-process subprocess on the Spark driver's bundled .NET 8
    runtime. Per-run forensic artifacts (executions CSV, trace CSV,
    result.json, `*.log`) stay under `/tmp/fdlt-<RunId>/`.
    """)

    # 1. Configuration
    code(nb, rf"""
# ── 1. Configuration ──────────────────────────────────────────────────────────
# All knobs the load test reads live here. For most runs you only need to
# touch a few — see the "essential" section below. Full reference for every
# parameter (semantics, defaults, examples) is in:
#
#     docs/loadgen-main.md
#     https://github.com/dbrownems/FabricDaxLoadTest/blob/main/docs/loadgen-main.md
#
# ─────────────── Essential parameters (typical run) ───────────────────────────

# Target semantic model (None → only model in current workspace)
TARGET_DATASET   = None
TARGET_WORKSPACE = None

# Load shape
DURATION_SECONDS   = 60
CONCURRENT_USERS   = 25
USER_RAMP_TIME_SEC = 15

# Optional: persist the run to Delta tables for cross-run analysis.
# Leave None for the simplest case — charts read the local CSV.
LAKEHOUSE_NAME = None

# Scenario (queries to drive). Leave QUERIES_FILE = None to auto-pick the
# single .json attached to the notebook's *Resources* panel (e.g. a
# Power BI Performance Analyzer export). QUERIES_INLINE is the fallback.
QUERIES_FILE   = None
QUERIES_INLINE = [
    "EVALUATE ROW(\"ping\", 1)",
    "EVALUATE INFO.TABLES()",
    "EVALUATE INFO.MEASURES()",
]

# ─────────────── Advanced parameters (see docs/loadgen-main.md) ───────────────

TARGET_REPLICA               = ""        # "readonly" → scale-out read replica
LAKEHOUSE_WORKSPACE_NAME     = None      # for BYO-lakehouse in another workspace
LAKEHOUSE_SCHEMA             = None      # None → auto-detect (schema-enabled → "dbo")

CONCURRENT_QUERIES_PER_USER  = 1         # in-flight queries per user (1 = serial)
PAUSE_BETWEEN_ITERATIONS_MS  = 1000      # think-time between iterations
PAUSE_BETWEEN_QUERIES_MS     = 0         # think-time between queries in an iteration

ENABLE_TRACING               = True      # capture engine events to TraceEvents
SKIP_RESULTS                 = False     # True → drain rows without parsing

USERS_FILE                   = None      # RLS / impersonation list
USERS_INLINE                 = []        # see docs/impersonation.md

LOG_FOLDER                   = None      # None → /tmp on driver; "abfss://…" or local path supported

# Runtime wheel — to upgrade, bump the version (e.g. v0.9.0 → v0.10.0) and Run-All.
WHEEL_URL = "{WHEEL_URL_DEFAULT}"
""")

    # 2. Bootstrap — pip-install the fdlt_runtime wheel and call bootstrap.
    code(nb, r"""
# ── 2. Bootstrap: pip install the fdlt_runtime wheel and call bootstrap ──────
# As of v0.5.0 the .NET LoadGen binaries ship inside the wheel, so this
# is the entire deploy. WHEEL_URL is set in cell 1 — to upgrade, edit
# the version there and Run-All.

# The sentinel literal is constructed at runtime so the WHEEL_URL line
# in cell 1 is the *only* occurrence of the literal in the notebook source.
# That lets scripts/Deploy-LoadTests.ps1 do a blunt string-replace
# without nuking the comparison value too.
_SENTINEL = "REPLACE_ME" + "_WITH_WHEEL_URL"
if WHEEL_URL == _SENTINEL:
    raise RuntimeError(
        "Cell 1: WHEEL_URL was not patched. Either re-run the GitHub "
        "release workflow (sets FDLT_RELEASE_VERSION env var), run "
        "scripts/Deploy-LoadTests.ps1 (patches the URL to the locally "
        "uploaded wheel), or paste a release wheel URL by hand.")

import importlib, json, os, subprocess, sys, urllib.request
import notebookutils

# `*.*.*` is a "always use latest release" opt-in. Resolve it to the
# current GitHub release tag (e.g. v0.9.2) before any other URL
# handling. One GET against the public releases API; failures bubble
# up so the user knows their wildcard didn't resolve (vs. silently
# falling back to a stale pin).
if "*.*.*" in WHEEL_URL:
    _api = "https://api.github.com/repos/dbrownems/FabricDaxLoadTest/releases/latest"
    print(f"WHEEL_URL contains *.*.* — resolving latest release from {_api}")
    with urllib.request.urlopen(_api, timeout=30) as _r:
        _tag = json.loads(_r.read().decode("utf-8"))["tag_name"]
    _ver = _tag.lstrip("v")
    WHEEL_URL = WHEEL_URL.replace("v*.*.*", _tag).replace("*.*.*", _ver)
    print(f"Resolved WHEEL_URL = {WHEEL_URL}")

if WHEEL_URL.startswith("abfss://"):
    # pip requires wheel filenames to match PEP 427 (name-version-...-py3-none-any.whl);
    # strip the path component but keep the filename verbatim.
    _local = "/tmp/" + WHEEL_URL.rsplit("/", 1)[-1]
    if os.path.exists(_local):
        os.remove(_local)
    notebookutils.fs.cp(WHEEL_URL, "file://" + _local)
    _src = _local
else:
    # https://, /lakehouse/... or any other path pip already understands.
    _src = WHEEL_URL

# --no-deps: fdlt_runtime declares no Python deps today (everything it
# needs is already in the Fabric Spark image). Drop --no-deps if that
# ever changes. --force-reinstall: ensure the kernel picks up the
# just-fetched wheel even if a cached copy of the same version is
# already installed.
_pip = subprocess.run(
    [sys.executable, "-m", "pip", "install", "--quiet",
     "--force-reinstall", "--no-deps", _src],
    capture_output=True, text=True, timeout=180)
if _pip.returncode != 0:
    raise RuntimeError(
        f"pip install of {_src} failed:\n{_pip.stderr or _pip.stdout}")

# Purge any cached fdlt_runtime modules so the import picks up the
# just-installed wheel (relevant on subsequent Run-All cycles when the
# kernel is reused but WHEEL_URL was bumped).
for _m in [m for m in list(sys.modules)
           if m == "fdlt_runtime" or m.startswith("fdlt_runtime.")]:
    del sys.modules[_m]
importlib.invalidate_caches()

import fdlt_runtime
from fdlt_runtime import notebook as fdlt_nb

boot = fdlt_nb.bootstrap(
    lakehouse_name=LAKEHOUSE_NAME,
    lakehouse_workspace=LAKEHOUSE_WORKSPACE_NAME,
    lakehouse_schema=LAKEHOUSE_SCHEMA,
)
""")

    # 3. Run + persist — single call into fdlt_runtime.notebook.run()
    code(nb, r"""
# ── 3. Run the load test and persist results ─────────────────────────────────
# Thin shim: every parameter from cell 1 is passed by keyword to
# `fdlt_runtime.notebook.run()`, which loads the scenario, resolves
# the target, runs LoadGen, streams progress, and writes 4 Delta tables.
# Press the ■ Interrupt Kernel button to cancel.
outcome = fdlt_nb.run(
    boot,
    target_workspace=TARGET_WORKSPACE,
    target_dataset=TARGET_DATASET,
    target_replica=TARGET_REPLICA,
    duration_seconds=DURATION_SECONDS,
    concurrent_users=CONCURRENT_USERS,
    concurrent_queries_per_user=CONCURRENT_QUERIES_PER_USER,
    pause_between_iterations_ms=PAUSE_BETWEEN_ITERATIONS_MS,
    pause_between_queries_ms=PAUSE_BETWEEN_QUERIES_MS,
    user_ramp_time_sec=USER_RAMP_TIME_SEC,
    skip_results=SKIP_RESULTS,
    enable_tracing=ENABLE_TRACING,
    queries_file=QUERIES_FILE,
    queries_inline=QUERIES_INLINE,
    users_file=USERS_FILE,
    users_inline=USERS_INLINE,
    log_folder=LOG_FOLDER,
    spark=spark,
)
""")

    # 4. Analyze (charts)
    code(nb, r"""
# ── 4. Charts ────────────────────────────────────────────────────────────────
# Latency band + QPS + active-user figure from the per-run CSV.
fdlt_nb.analyze(outcome)
""")

    write(nb, OUT / "LoadTest-Main.ipynb")


if __name__ == "__main__":
    build_run()
