"""Generate notebooks/LoadTest-Main.ipynb.

The notebook is deployed into a `LoadTests` workspace folder by
scripts/Deploy-LoadTests.ps1, alongside a `LoadTests` lakehouse that
holds per-run telemetry under Files/runs/ and the four Delta tables
under Tables[/dbo]/.

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

    **This is your Load Test.** Edit cell 1 and run it. Each Run-All mints a
    fresh `RunId`, so re-running is purely additive — every Run is preserved
    in the four Delta tables for cross-Run comparison.

    > 🆕 **Multiple Load Tests in one workspace?** Most workspaces only need
    > one Load Test, and this is it. If you need *additional* Load Tests
    > (e.g. a baseline vs. a what-if scenario, or one per model under test),
    > **File → Save As** (or right-click → **Duplicate**) and rename the
    > copy to `LoadTest - <descriptive name>` — keep it in the same
    > `LoadTests` folder so it can find the lakehouse.
    >
    > **What about upgrades?** Change the `WHEEL_URL` in cell 2 to a newer
    > release (e.g. bump `v0.5.0` → `v0.6.0`) and Run All — that's the entire
    > upgrade story. The .NET LoadGen binaries ship inside the
    > `fdlt_runtime` wheel, so there is nothing else to refresh.
    > `scripts/Deploy-LoadTests.ps1` will *not* overwrite this notebook
    > (or any saved `LoadTest - …` copy) if it already exists, so cell-1
    > edits are preserved across redeploys.

    ---

    Drives concurrent DAX queries against a Power BI / Fabric semantic model via
    the **XMLA endpoint** by launching `LoadGen.dll` as an out-of-process
    subprocess (run on the Spark driver's bundled .NET 8 runtime).

    The notebook discovers the destination lakehouse via the
    `LAKEHOUSE_WORKSPACE_NAME` / `LAKEHOUSE_NAME` parameters in cell 1
    (defaults: current workspace, `LoadTests`). The lakehouse holds:

    - `Files/runs/`  — per-run telemetry CSVs (created on first run)
    - `Tables[/dbo]/LoadTest{s,Runs,Queries,QueryExecutions,TraceEvents}` —
      Delta tables written at end-of-run. The `dbo/` prefix is added
      automatically when the lakehouse is schema-enabled; flat lakehouses
      write directly under `Tables/`. Override with `LAKEHOUSE_SCHEMA` in
      cell 1.

    > Note: pre-v0.5.0 deployments staged a `Files/loadgen-bin.zip` here too;
    > that file is no longer used and is safe to delete.

    ## How to use

    1. **Set up the Scenario.** Cell 1 ships with a tiny `QUERIES_INLINE`
       fallback (3 model-agnostic warm-up queries) — *only* useful for smoke
       testing the pipeline. For a real test you have two options:

       - **Drop a `.json` onto the notebook's *Resources* panel** (left
         sidebar). If exactly one `.json` is attached, cell 3 picks it up
         automatically; otherwise set `QUERIES_FILE = "name.json"` in cell 1.
         Accepted shapes: Power BI Desktop *Performance Analyzer* export,
         `[{"query": "EVALUATE …"}, …]`, or `["EVALUATE …", …]`.
       - **Edit `QUERIES_INLINE` in cell 1** with the DAX you want to drive.
         Fine for one-off tests; doesn't scale to large Scenarios.

       Optional: drop a `users.json` onto Resources too and set
       `USERS_FILE = "users.json"` in cell 1 to drive role / EffectiveUserName
       impersonation. Accepted shapes: `[{"email": "...", "role": "..."}, …]`
       or `["alice@contoso.com", "bob@contoso.com"]`.

    2. Edit cell **1** to point at the target workspace + dataset and tweak
       load parameters. `LOAD_TEST_NAME` defaults to the notebook name with
       any `LoadTest -` prefix stripped (so `LoadTest - Main` → `Main`); set
       it explicitly if you want a different label in the `LoadTests` dim.
    3. **Run All**. Cell **3** prints a live status line every second and
       then writes the Run into the four Delta tables; press **Interrupt
       Kernel** (■) to cancel — the subprocess receives SIGINT and drains
       cleanly. Each Run-All mints a fresh `RunId`, so prior Runs are
       preserved untouched and re-running is purely additive.
    4. Cell **4** plots latency / QPS / users for the Run that just
       completed, straight from the per-run CSV.

    > Re-deploy / upgrade the wheel by re-running
    > `scripts/Deploy-LoadTests.ps1` from a clone of the repo, or by editing
    > `WHEEL_URL` in cell 2 to a newer GitHub release. This notebook
    > and any saved `LoadTest - …` copies are **not** touched by the deploy.
    """)

    # 1. Configuration
    code(nb, r"""
# ── 1. Configuration ──────────────────────────────────────────────────────────
# Every knob the load test reads lives here. Cell 2 bootstraps the runtime
# without touching any of these; cell 3 consumes them.

# ── Identity (human labels written to LoadTests / LoadTestRuns) ──────────────
LOAD_TEST_NAME        = None             # short label — PK into LoadTests table
                                         #   None → derived from notebook name
                                         #          ("LoadTest - Foo" → "Foo")
LOAD_TEST_DESCRIPTION = ""               # optional free-text notes for the run

# ── Target semantic model ────────────────────────────────────────────────────
TARGET_WORKSPACE = None  # workspace hosting the model under test
                         #   None         → use the workspace this notebook lives in
                         #   "Name"/GUID  → cross-workspace test (XMLA endpoint)
TARGET_DATASET   = None  # semantic model display name
                         #   None         → auto-pick the *only* semantic model in
                         #                  TARGET_WORKSPACE; error if 0 or >1
                         #   "Name"       → exact display-name match
TARGET_REPLICA   = ""    # XMLA replica hint (no replica unless set)
                         #   ""           → primary replica (default)
                         #   "readonly"   → route to a scale-out read replica

# ── Load shape ───────────────────────────────────────────────────────────────
DURATION_SECONDS             = 60     # how long virtual users execute queries
CONCURRENT_USERS             = 25     # max concurrency at steady state
USER_RAMP_TIME_SEC           = 15     # linear ramp from 0 → CONCURRENT_USERS
QUERIES_PER_BATCH            = 1      # queries per user iteration (>1 = bursts)
PAUSE_BETWEEN_ITERATIONS_MS  = 1000   # think-time between batches per user
PAUSE_BETWEEN_QUERIES_MS     = 0      # think-time between queries inside a batch
SKIP_RESULTS                 = False  # True → drain rows without parsing
                                      #   useful for stress-testing the engine
                                      #   when result-set parsing would dominate
ENABLE_TRACING               = True   # subscribe to dataset XMLA trace and
                                      #   capture engine events (QueryEnd,
                                      #   ExecutionMetrics, VertiPaq SE) into
                                      #   the LoadTestTraceEvents Delta table.
                                      #   Requires Build/Read on the dataset.
                                      #   Set False to skip tracing entirely.

# ── Load Test Scenario (queries) ─────────────────────────────────────────────
# QUERIES_FILE — name of a .json in this notebook's *Resources* panel
# (left sidebar). Cell 3 resolves it in this order:
#
#   1. None  → if exactly one `*.json` is in Resources, use that file.
#   2. "name.json" → load `builtin/name.json` from Resources.
#   3. "abfss://…" → cross-lakehouse / cross-workspace escape hatch.
#   4. Nothing matches → fall back to `QUERIES_INLINE` below.
#
# Accepted JSON shapes (see README → "Load Test Scenario formats"):
#   • Power BI Desktop *Performance Analyzer* export (with `events[]`)
#   • [{"query": "EVALUATE …"}, …]
#   • ["EVALUATE …", …]
QUERIES_FILE = None       # auto-pick single .json in Resources

# Fallback used only when no resource file is attached. The default is a
# tiny model-agnostic warm-up scenario useful only for smoke-testing the
# pipeline — replace with real DAX, or attach a Performance Analyzer
# export to the Resources panel, before drawing any conclusions.
QUERIES_INLINE = [
    "EVALUATE ROW(\"ping\", 1)",
    "EVALUATE INFO.TABLES()",
    "EVALUATE INFO.MEASURES()",
]

# ── Virtual users (optional impersonation list) ──────────────────────────────
# USERS_FILE — name of a .json in this notebook's Resources panel
# describing virtual-user identities for AS `EffectiveUserName=` /
# `Roles=` impersonation. Resolution order:
#
#   1. None       → use `USERS_INLINE` below (or a single anonymous user).
#   2. "name.json" → load `builtin/name.json` from Resources.
#   3. "abfss://…" → cross-lakehouse escape hatch.
#
# Auto-discovery does NOT pick up a stray .json for users — single-.json
# Resources always go to QUERIES_FILE. Users must be named explicitly.
#
# Accepted JSON shapes (see README → "User list formats"):
#   • [{"email": "alice@contoso.com", "role": "Sales"}, …]
#   • ["alice@contoso.com", "bob@contoso.com"]   (roles default to "")
USERS_FILE   = None
USERS_INLINE = []   # empty ⇒ all virtual users share the notebook token

# ── Lakehouse (where the 4 Delta tables are written) ─────────────────────────
LAKEHOUSE_WORKSPACE_NAME = None  # workspace hosting the destination lakehouse
                                 #   None  → current workspace (default;
                                 #            common case — `Deploy-LoadTests.ps1`
                                 #            puts the lakehouse alongside this
                                 #            notebook)
                                 #   "name"/GUID → BYO-lakehouse: point at any
                                 #            lakehouse you have Build access to,
                                 #            in any workspace on this tenant
LAKEHOUSE_NAME   = "LoadTests"  # display name of the destination lakehouse
                                #   created by scripts/Deploy-LoadTests.ps1
                                #   override for BYO-lakehouse scenarios
LAKEHOUSE_SCHEMA = None         # destination schema for the 4 Delta tables
                                #   None        → auto-detect via Fabric API
                                #                 (schema-enabled → "dbo",
                                #                  flat lakehouse → "")
                                #   "dbo"/other → force Tables/<name>/
                                #   ""          → force flat Tables/
""")

    # 2. Bootstrap — pip-install the fdlt_runtime wheel and call bootstrap.
    code(nb, rf"""
# ── 2. Bootstrap: pip install the fdlt_runtime wheel and call bootstrap ──────
# As of v0.5.0 the .NET LoadGen binaries ship inside the wheel, so this
# is the entire deploy. To upgrade, change WHEEL_URL to a newer release
# tag (e.g. v0.5.0 → v0.6.0) and Run-All.
#
# WHEEL_URL forms supported:
#   - https://github.com/dbrownems/FabricDaxLoadTest/releases/download/vX.Y.Z/fdlt_runtime-X.Y.Z-py3-none-any.whl
#       (default — direct from GitHub; needs outbound internet from Spark)
#   - abfss://<wsid>@onelake.dfs.fabric.microsoft.com/<lhid>/Files/<file>.whl
#       (offline-friendly — set by scripts/Deploy-LoadTests.ps1)
#   - /lakehouse/default/Files/<file>.whl
#       (already-attached lakehouse, manual upload)
WHEEL_URL = "{WHEEL_URL_DEFAULT}"

# The sentinel literal is constructed at runtime so the WHEEL_URL line
# above is the *only* occurrence of the literal in the notebook source.
# That lets scripts/Deploy-LoadTests.ps1 do a blunt string-replace
# without nuking the comparison value too.
_SENTINEL = "REPLACE_ME" + "_WITH_WHEEL_URL"
if WHEEL_URL == _SENTINEL:
    raise RuntimeError(
        "Cell 2: WHEEL_URL was not patched. Either re-run the GitHub "
        "release workflow (sets FDLT_RELEASE_VERSION env var), run "
        "scripts/Deploy-LoadTests.ps1 (patches the URL to the locally "
        "uploaded wheel), or paste a release wheel URL by hand.")

import importlib, os, subprocess, sys
import notebookutils

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
        f"pip install of {{_src}} failed:\n{{_pip.stderr or _pip.stdout}}")

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
    load_test_name=LOAD_TEST_NAME,
    load_test_description=LOAD_TEST_DESCRIPTION,
    target_workspace=TARGET_WORKSPACE,
    target_dataset=TARGET_DATASET,
    target_replica=TARGET_REPLICA,
    duration_seconds=DURATION_SECONDS,
    concurrent_users=CONCURRENT_USERS,
    queries_per_batch=QUERIES_PER_BATCH,
    pause_between_iterations_ms=PAUSE_BETWEEN_ITERATIONS_MS,
    pause_between_queries_ms=PAUSE_BETWEEN_QUERIES_MS,
    user_ramp_time_sec=USER_RAMP_TIME_SEC,
    skip_results=SKIP_RESULTS,
    enable_tracing=ENABLE_TRACING,
    queries_file=QUERIES_FILE,
    queries_inline=QUERIES_INLINE,
    users_file=USERS_FILE,
    users_inline=USERS_INLINE,
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
