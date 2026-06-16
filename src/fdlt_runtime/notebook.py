"""Thin orchestrator API for saved LoadTest notebooks.

Saved `LoadTest-<name>.ipynb` files are near-immutable shims that
import this module and call `bootstrap()` → `run()` → `analyze()`.
That way a `Deploy-LoadTests.ps1` redeploy can ship behavior changes
in the wheel without users having to re-save their notebooks.

Forward-compatibility contract:
- `bootstrap()` and `run()` accept all options as keyword arguments
  with safe defaults. New parameters MUST default to a value that
  preserves prior behavior so older saved notebooks keep working.
- `BootstrapResult` and `RunOutcome` are dataclasses; new fields MUST
  be appended (never reorder, never rename) so attribute access from
  older notebooks remains valid.
"""
from __future__ import annotations

import glob
import os
from dataclasses import dataclass
from importlib.resources import files
from typing import Any, Mapping, Optional, Sequence

from .env import (
    LakehouseInfo,
    discover_lakehouse,
    find_dotnet,
    resolve_target_dataset,
    resolve_workspace,
)
from .persist import WriteSummary, write_run
from .queries import load_queries, load_users
from .runner import RunConfig, RunResult, render_progress, run_load_test


@dataclass
class BootstrapResult:
    """Output of `bootstrap()` — pass into `run()`."""

    ctx: dict
    workspace_id: str
    workspace_name: str
    notebook_name: str
    token: str
    lakehouse: Optional[LakehouseInfo]
    dotnet: str
    loadgen_dll: str
    runtime_version: str


@dataclass
class RunOutcome:
    """Output of `run()` — pass into `analyze()`."""

    result: RunResult
    write_summary: Optional[WriteSummary]
    load_test_name: str
    cfg: RunConfig
    run_dest: str


def bootstrap(
    *,
    lakehouse_name: Optional[str] = None,
    lakehouse_workspace: Optional[str] = None,
    lakehouse_schema: Optional[str] = None,
) -> BootstrapResult:
    """Resolve env (workspace / lakehouse / dotnet) for a load test.

    `lakehouse_name` is **optional**. When supplied, the lakehouse is
    located in `lakehouse_workspace` (display name or GUID; defaults
    to the notebook's workspace) and used as the destination for the
    6 Delta tables written after each run. **When omitted, persistence
    is skipped entirely** — the load test still runs, plots still
    render (they read the local LoadGen CSV directly, no Spark
    needed), but no Delta tables are created and no SQL-endpoint sync
    is attempted. This is the cheapest quickstart for a one-off "is
    my model fast enough?" test.

    Locates `LoadGen.dll` inside the installed `fdlt_runtime` wheel
    via `importlib.resources` — pip-installing the wheel is the
    entire deploy. Cell 2 of the notebook is just `pip install <wheel>`
    followed by this call.
    """
    import notebookutils  # type: ignore[import-not-found]

    from . import __version__

    ctx = dict(notebookutils.runtime.context)
    ws_id = ctx["currentWorkspaceId"]
    ws_name = ctx.get("currentWorkspaceName") or ws_id
    nb_name = (ctx.get("currentNotebookName") or "").strip()

    token = notebookutils.credentials.getToken("pbi")

    lh: Optional[LakehouseInfo] = None
    if lakehouse_name:
        # Resolve the lakehouse's workspace. None => same as notebook.
        if lakehouse_workspace is None:
            lh_ws_id, lh_ws_name = ws_id, ws_name
        else:
            lh_ws_id, lh_ws_name = resolve_workspace(lakehouse_workspace, token)
        lh = discover_lakehouse(
            workspace_id=lh_ws_id, lakehouse_name=lakehouse_name,
            token=token, workspace_name=lh_ws_name,
            schema_override=lakehouse_schema,
            list_tables=lambda p: notebookutils.fs.ls(p))

    dotnet = find_dotnet()
    loadgen_dll = os.fspath(files("fdlt_runtime").joinpath("loadgen", "LoadGen.dll"))
    if not os.path.exists(loadgen_dll):
        raise FileNotFoundError(
            "LoadGen.dll is missing from the installed fdlt_runtime "
            f"wheel (expected at {loadgen_dll}). The wheel was built "
            "without the bundled .NET binaries — re-run "
            "scripts/Deploy-LoadTests.ps1, or download a release wheel "
            "from https://github.com/dbrownems/FabricDaxLoadTest/releases.")

    print(f"Workspace : {ws_name} ({ws_id})")
    if lh is None:
        print("Lakehouse : (none — persistence disabled, plots will read local CSV)")
    else:
        if lh.workspace_id != ws_id:
            print(f"Lakehouse-WS: {lh.workspace_name} ({lh.workspace_id})  "
                  "(BYO — different from notebook workspace)")
        print(f"Lakehouse : {lh.lakehouse_name} ({lh.lakehouse_id})  "
              f"schema={lh.schema or '(flat / no schema)'}")
    print(f"LoadGen   : {loadgen_dll}  ({os.path.getsize(loadgen_dll):,} bytes)")
    print(f"Runtime   : fdlt_runtime {__version__}")
    print(f"dotnet    : {dotnet}")
    return BootstrapResult(
        ctx=ctx, workspace_id=ws_id, workspace_name=ws_name,
        notebook_name=nb_name, token=token, lakehouse=lh,
        dotnet=dotnet, loadgen_dll=loadgen_dll, runtime_version=__version__)


def run(
    boot: BootstrapResult,
    *,
    # Target
    target_workspace: Optional[str] = None,
    target_dataset: Optional[str] = None,
    target_replica: str = "",
    # Load shape
    duration_seconds: int = 60,
    concurrent_users: int = 25,
    concurrent_queries_per_user: int = 1,
    pause_between_iterations_ms: int = 10000,
    pause_between_queries_ms: int = 0,
    user_ramp_time_sec: int = 15,
    skip_results: bool = False,
    enable_tracing: bool = True,
    # Scenario
    queries_file: Optional[str] = None,
    queries_inline: Optional[Sequence[str]] = None,
    users_file: Optional[str] = None,
    users_inline: Optional[Sequence[Mapping[str, str]]] = None,
    # Where LoadGen writes its artifacts (CSV/trace/log)
    log_folder: Optional[str] = None,
    # Spark for the Delta writer (auto-fetched if None)
    spark: Any = None,
) -> RunOutcome:
    """Resolve target + run LoadGen + persist Delta tables. One-shot.

    ``log_folder`` controls where the LoadGen subprocess writes its
    artifacts:
      * ``None`` (default) → ``/tmp/fdlt-run-<id>`` on the Spark driver;
        a post-run copy of ``*.log`` / ``*.trace.csv`` lands under
        ``{default_lakehouse}/Files/run-logs/<run_id>/``.
      * Local path (e.g. ``/lakehouse/default/Files/loadtest-logs``) →
        LoadGen writes there directly. No post-run copy needed; the
        artifacts already live in OneLake.
      * ``abfss://...`` URL → LoadGen still writes to /tmp (the .NET
        process can't target OneLake directly), but the post-run copy
        targets ``<abfss>/<run_id>/`` instead of the default
        Files/run-logs/ location.
    """
    import notebookutils  # type: ignore[import-not-found]

    from . import __version__

    def _read_abfss(path: str) -> str:
        """Read a OneLake abfss:// file as text, full contents.

        Uses Spark's ``read.text(..., wholetext=True)`` because
        ``notebookutils.fs.head`` has an internal ~100 KB cap that
        ignores its ``max_bytes`` argument — large trace JSONL files
        get silently truncated mid-line, producing fewer (or zero)
        queries with no error. Spark handles abfss auth via the
        session's AAD token, same as ``fs.head`` would.
        """
        s = spark
        if s is None:
            from pyspark.sql import SparkSession  # type: ignore
            s = SparkSession.builder.getOrCreate()
        df = s.read.text(path, wholetext=True)
        rows = df.collect()
        if not rows:
            raise RuntimeError(f"Read 0 rows from {path!r}")
        return rows[0][0]

    queries, query_visuals, q_src = load_queries(
        queries_file, queries_inline or [], read_abfss=_read_abfss)
    print(f"Queries   : {len(queries)}  from {q_src}")
    if q_src.startswith("(QUERIES_INLINE"):
        print("          ⚠ no resource file attached — using fallback queries only.")
        print("          Drop a Performance Analyzer .json or trace .jsonl onto Resources for a real test.")
    bound_visuals = sum(1 for v in query_visuals if v is not None)
    if bound_visuals:
        print(f"Visuals   : {bound_visuals}/{len(queries)} queries bound to a Power BI visual")
    base, u_src = load_users(
        users_file, users_inline or [], read_abfss=_read_abfss)
    print(f"Users     : pool={len(base)}  from {u_src}")
    users = [base[i % len(base)] for i in range(concurrent_users)]

    # Resolve TARGET_WORKSPACE → (id, name); default is the notebook's workspace.
    if target_workspace is None:
        tgt_ws_id, tgt_ws_name = boot.workspace_id, boot.workspace_name
    else:
        tgt_ws_id, tgt_ws_name = resolve_workspace(target_workspace, boot.token)
    ds = resolve_target_dataset(
        workspace_id=tgt_ws_id, token=boot.token,
        workspace_name=tgt_ws_name, dataset_name=target_dataset)
    print(f"Target    : {ds.workspace_name} / {ds.dataset_name}")

    resolved_name = boot.notebook_name or "my-load-test"
    print(f"LoadTest  : {resolved_name}")

    cfg = RunConfig(
        load_test_name=resolved_name,
        target_workspace=tgt_ws_name,
        target_dataset=ds.dataset_name,
        target_replica=target_replica,
        duration_seconds=int(duration_seconds),
        concurrent_users=int(concurrent_users),
        concurrent_queries_per_user=int(concurrent_queries_per_user),
        pause_between_iterations_ms=int(pause_between_iterations_ms),
        pause_between_queries_ms=int(pause_between_queries_ms),
        user_ramp_time_sec=int(user_ramp_time_sec),
        skip_results=bool(skip_results),
        enable_tracing=bool(enable_tracing),
        queries=queries, users=users,
        token=boot.token,
    )
    print(f"Endpoint  : {cfg.xmla}"
          f"{('?' + target_replica) if target_replica else ''}")
    print(f"Shape     : {len(users)} users, ramp {user_ramp_time_sec}s, "
          f"dur {duration_seconds}s")

    on_status = _make_on_status()

    # log_folder semantics:
    #   None / "abfss://..."  →  LoadGen writes to /tmp (driver-local)
    #   local path            →  LoadGen writes there directly (live to OneLake
    #                            via /lakehouse/default mount etc.)
    runner_log_folder = None
    if log_folder and not log_folder.startswith("abfss://"):
        runner_log_folder = log_folder

    rr = run_load_test(
        cfg, dotnet=boot.dotnet, loadgen_dll=boot.loadgen_dll,
        on_status=on_status, log_folder=runner_log_folder)

    # Forensic artifacts (executions CSV, trace CSV, result.json, *.log)
    # stay on the Spark driver's local disk under run_local_dir. We
    # deliberately do NOT copy them to OneLake — everything the analytics
    # layer needs already lives in the 6 Delta tables. Run the load test
    # again and you'll get a fresh /tmp dir; if you need the raw artifacts
    # for forensics, grab them from `run_dest` before the kernel cycles.
    run_dest = rr.run_local_dir

    _print_run_banner(rr, run_dest)

    write_summary: Optional[WriteSummary] = None
    if rr.run_id and boot.lakehouse is not None:
        if spark is None:
            from pyspark.sql import SparkSession  # type: ignore
            spark = SparkSession.builder.getOrCreate()
        write_summary = write_run(
            spark, table_base=boot.lakehouse.table_base,
            workspace_id=boot.workspace_id, workspace_name=boot.workspace_name,
            notebook_id=(boot.ctx.get("currentNotebookId")
                         or boot.ctx.get("notebookId")),
            notebook_name=boot.notebook_name or resolved_name,
            load_test_name=resolved_name,
            target_workspace=tgt_ws_name, target_dataset=ds.dataset_name,
            target_replica=target_replica, xmla=cfg.xmla,
            queries=queries, query_visuals=query_visuals, user_count=concurrent_users,
            duration_sec=duration_seconds, ramp_sec=user_ramp_time_sec,
            concurrent_queries_per_user=concurrent_queries_per_user,
            pause_iter_ms=pause_between_iterations_ms,
            pause_query_ms=pause_between_queries_ms,
            skip_results=skip_results,
            run=rr, runtime_version=__version__,
        )
        print("\n=== Lakehouse write OK ===")
        print(f"  LoadTestId : {write_summary.load_test_id}")
        print(f"  RunId      : {write_summary.run_id}")
        print(f"  Queries    : {write_summary.queries_written} "
              f"(scenario hash {write_summary.scenario_hash[:12]}...)")
        print(f"  Executions : {write_summary.executions_written:,}")
        print(f"  Trace evts : {write_summary.trace_events_written:,}")
        print("  Tables     : LoadTests, LoadTestRuns, Queries, "
              "QueryVisuals, QueryExecutions, TraceEvents")
        print(f"  Lakehouse  : {boot.lakehouse.lakehouse_name} "
              f"({boot.lakehouse.lakehouse_id})  "
              f"base={boot.lakehouse.table_base}")

        # Force the SQL analytics endpoint to refresh its catalog so the
        # tables we just wrote are queryable from T-SQL / Direct Lake
        # immediately, without waiting for the background metadata sync.
        sep_id = boot.lakehouse.sql_endpoint_id
        if sep_id:
            try:
                from .env import refresh_sql_endpoint_metadata
                resp = refresh_sql_endpoint_metadata(
                    boot.lakehouse.workspace_id, sep_id, boot.token)
                code = resp.get("status_code")
                if code == 200:
                    body = resp.get("body") or {}
                    statuses = body.get("value") or []
                    ok = sum(1 for s in statuses if s.get("status") == "Success")
                    print(f"  SQL sync   : refreshed {ok}/{len(statuses)} tables "
                          f"({sep_id})")
                else:
                    print(f"  SQL sync   : accepted (LRO, {sep_id})")
            except Exception as ex:  # noqa: BLE001 — best-effort
                print(f"  SQL sync   : (warning: refreshMetadata failed: {ex})")
        else:
            print("  SQL sync   : (skipped — no sqlEndpointId on lakehouse)")

        # Persist LoadGen *.log + *.trace.csv so they survive kernel
        # cycles. Routing depends on log_folder:
        #   None              → copy to {default_lh}/Files/run-logs/<run_id>/
        #   "abfss://..."     → copy to <log_folder>/<run_id>/
        #   local path        → SKIP (LoadGen already wrote them there)
        if log_folder and not log_folder.startswith("abfss://"):
            print(f"  Log copy   : (skipped — LoadGen wrote directly to {log_folder})")
        else:
            try:
                _persist_run_logs(
                    rr, boot.lakehouse, write_summary.run_id,
                    dest_override=log_folder,  # None or abfss://
                )
            except Exception as ex:  # noqa: BLE001 — best-effort
                print(f"  Log copy   : (warning: {ex})")
    elif rr.run_id:
        # No lakehouse configured — persistence and log copy are skipped
        # by design. Forensic artifacts still live at run_dest on the
        # Spark driver's local disk for the lifetime of the session.
        print("\n=== Lakehouse write skipped (no lakehouse configured) ===")
        print(f"  Run artifacts : {run_dest}  (driver-local, lost on session end)")
        print("  Plots         : will read directly from local CSV — no Spark/Delta needed")

    return RunOutcome(
        result=rr, write_summary=write_summary,
        load_test_name=resolved_name, cfg=cfg, run_dest=run_dest)


def _persist_run_logs(rr: RunResult, lh, run_id: str,
                      dest_override: Optional[str] = None) -> None:
    """Copy *.log + *.trace.csv from rr.run_local_dir to OneLake.

    Default destination: ``{lakehouse-abfss}/Files/run-logs/{run_id}/``.
    Pass ``dest_override="abfss://.../some/folder"`` to redirect to an
    arbitrary lakehouse folder; the run_id is appended as a
    subdirectory. Uses ``notebookutils.fs.cp`` when available (the
    standard Fabric notebook path); falls back to the
    ``/lakehouse/default/Files/...`` mount when it's the configured
    default lakehouse. Caller wraps in try/except.
    """
    import glob
    import os
    src_dir = rr.run_local_dir
    if not src_dir or not os.path.isdir(src_dir):
        return
    files = (sorted(glob.glob(os.path.join(src_dir, "*.log"))) +
             sorted(glob.glob(os.path.join(src_dir, "*.trace.csv"))))
    if not files:
        return
    if dest_override:
        # Strip any trailing slash from the user's folder URL so we
        # don't end up with a double-slash before the run_id segment.
        dest_dir_abfss = f"{dest_override.rstrip('/')}/{run_id}"
    else:
        dest_dir_abfss = f"{lh.abfss}/Files/run-logs/{run_id}"

    # Prefer notebookutils.fs.cp — it knows how to authenticate against
    # OneLake from the running Spark driver without an explicit token.
    try:
        import notebookutils  # type: ignore
        fs = notebookutils.fs
    except Exception:
        fs = None

    copied = 0
    if fs is not None:
        try:
            fs.mkdirs(dest_dir_abfss)
        except Exception:
            pass  # mkdirs may not exist or may fail if dir already exists
        for src in files:
            name = os.path.basename(src)
            try:
                fs.cp(f"file://{src}", f"{dest_dir_abfss}/{name}", True)
                copied += 1
            except Exception as ex:  # noqa: BLE001
                print(f"  Log copy   : (warning copying {name}: {ex})")
    else:
        # Fallback: write through the /lakehouse/default mount when the
        # current default lakehouse matches the destination.
        mount = f"/lakehouse/default/Files/run-logs/{run_id}"
        os.makedirs(mount, exist_ok=True)
        import shutil
        for src in files:
            shutil.copy2(src, os.path.join(mount, os.path.basename(src)))
            copied += 1

    if copied:
        print(f"  Log copy   : {copied} file(s) -> Files/run-logs/{run_id}/")


def _make_on_status():
    try:
        from IPython.display import display, update_display  # type: ignore
    except ImportError:
        def _on_status(env_obj):
            if env_obj.get("type") == "progress":
                print(render_progress(env_obj))
        return _on_status

    display({"text/plain": "Starting LoadGen ..."},
            raw=True, display_id="fdlt-status")

    def _on_status(env_obj):
        kind = env_obj.get("type")
        if kind == "progress":
            update_display({"text/plain": render_progress(env_obj)},
                           raw=True, display_id="fdlt-status")
        elif kind == "started" and env_obj.get("runId"):
            update_display({"text/plain": f"Started run {env_obj['runId']}"},
                           raw=True, display_id="fdlt-status")
        elif kind == "unknown":
            update_display(
                {"text/plain": f"(non-JSON stdout) {env_obj.get('raw','')}"},
                raw=True, display_id="fdlt-status")
    return _on_status


def _print_run_banner(rr: RunResult, run_dest: str) -> None:
    def _print_log_tail():
        try:
            logs = sorted(glob.glob(os.path.join(rr.run_local_dir, "*.log")))
            if logs:
                print(f"\n--- tail of {os.path.basename(logs[-1])} ---")
                with open(logs[-1], "r", encoding="utf-8",
                          errors="replace") as lf:
                    lines = lf.readlines()
                for line in lines[-100:]:
                    print(line.rstrip())
        except Exception as le:  # noqa: BLE001
            print(f"(could not read log file: {le})")

    if rr.error_envelope is not None or rr.returncode not in (0, 130):
        print()
        print("=== Load test FAILED ===")
        if rr.error_envelope is not None:
            print(f"code   : {rr.error_envelope.get('code')}")
            print(f"type   : {rr.error_envelope.get('exceptionType')}")
            print("message:")
            for ml in str(rr.error_envelope.get("message", "")).splitlines():
                print(f"  {ml}")
        print(f"exit code: {rr.returncode}")
        if rr.stderr_tail:
            print(f"\n--- LoadGen stderr (last "
                  f"{min(40, len(rr.stderr_tail))} lines) ---")
            for line in rr.stderr_tail[-40:]:
                print(line)
        _print_log_tail()
        print(f"\nRun artifacts (partial, driver-local): {run_dest}")
        raise RuntimeError(
            rr.error_envelope.get("message", "LoadGen exited non-zero")
            if rr.error_envelope
            else f"LoadGen exited with code {rr.returncode}")

    if rr.returncode == 130:
        print("\n=== Load test CANCELLED ===")
        if rr.result_envelope is None:
            if rr.stderr_tail:
                print(f"\n--- LoadGen stderr (last "
                      f"{min(20, len(rr.stderr_tail))} lines) ---")
                for line in rr.stderr_tail[-20:]:
                    print(line)
            _print_log_tail()
            print(f"\nRun artifacts: {run_dest}")

    if rr.result_envelope is not None:
        s = rr.result_envelope.get("summary", {}) or {}
        dur = s.get("duration", {}) or {}
        print()
        print("=== Results ===")
        print(f"Total executions : {s.get('totalExecutions')}")
        print(f"Successful       : {s.get('successfulExecutions')}")
        print(f"Failed           : {s.get('failedExecutions')}")
        print(f"QPS              : {s.get('qps')}")
        if dur:
            print(f"Duration (ms)    : min={dur.get('min')}  "
                  f"median={dur.get('median')}  mean={dur.get('mean')}  "
                  f"p95={dur.get('p95')}  p99={dur.get('p99')}  "
                  f"max={dur.get('max')}")
        print(f"\nFull result      : {rr.result_envelope.get('resultFile')}")
        print(f"Run artifacts    : {run_dest}")


def analyze(outcome: RunOutcome) -> Any:
    """Render the duration/QPS/active-users figure for a finished run."""
    from .analyze import plot_run

    fig = plot_run(
        outcome.result.csv_path,
        title=f"Run {outcome.result.run_id} — {outcome.load_test_name}",
        trace_csv_path=outcome.result.trace_csv_path,
    )
    # Don't call plt.show() — returning the Figure auto-displays it via
    # Jupyter's inline backend. Calling plt.show() AND returning fig would
    # render the chart twice in the cell output.
    return fig
