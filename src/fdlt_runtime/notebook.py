"""Thin orchestrator API for saved LoadTest notebooks.

Saved `LoadTest - <name>.ipynb` files are near-immutable shims that
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
    lakehouse: LakehouseInfo
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
    lakehouse_name: str,
    lakehouse_workspace: Optional[str] = None,
    lakehouse_schema: Optional[str] = None,
) -> BootstrapResult:
    """Resolve env (workspace / lakehouse / dotnet) for a load test.

    `lakehouse_name` is required — the destination lakehouse display
    name (cell-1 `LAKEHOUSE_NAME`). The lakehouse is searched in
    `lakehouse_workspace` (a workspace display name or GUID). If
    `lakehouse_workspace` is None, the current notebook's workspace is
    used (the common case). The discovered `(workspace_id,
    workspace_name, lakehouse_id, lakehouse_name)` tuple is exposed via
    `BootstrapResult.lakehouse` and is the single source of truth for
    every downstream OneLake/abfss path the notebook produces.

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

    # Resolve the lakehouse's workspace. None ⇒ same workspace as the notebook.
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


def _derive_load_test_name(explicit: Optional[str], nb_name: str) -> str:
    if explicit:
        return explicit
    if nb_name.lower().startswith("loadtest"):
        derived = nb_name[len("loadtest"):].lstrip(" -").strip()
        return derived or nb_name
    return nb_name or "my-load-test"


def run(
    boot: BootstrapResult,
    *,
    # Identity
    load_test_name: Optional[str] = None,
    load_test_description: str = "",
    # Target
    target_workspace: Optional[str] = None,
    target_dataset: Optional[str] = None,
    target_replica: str = "",
    # Load shape
    duration_seconds: int = 60,
    concurrent_users: int = 25,
    concurrent_queries_per_user: int = 1,
    pause_between_iterations_ms: int = 1000,
    pause_between_queries_ms: int = 0,
    user_ramp_time_sec: int = 15,
    skip_results: bool = False,
    enable_tracing: bool = True,
    # Scenario
    queries_file: Optional[str] = None,
    queries_inline: Optional[Sequence[str]] = None,
    users_file: Optional[str] = None,
    users_inline: Optional[Sequence[Mapping[str, str]]] = None,
    # Spark for the Delta writer (auto-fetched if None)
    spark: Any = None,
) -> RunOutcome:
    """Resolve target + run LoadGen + persist Delta tables. One-shot."""
    import notebookutils  # type: ignore[import-not-found]

    from . import __version__

    def _read_abfss(path: str) -> str:
        return notebookutils.fs.head(path, 1024 * 1024 * 4)

    queries, q_src = load_queries(
        queries_file, queries_inline or [], read_abfss=_read_abfss)
    print(f"Queries   : {len(queries)}  from {q_src}")
    if q_src.startswith("(QUERIES_INLINE"):
        print("          ⚠ no resource file attached — using fallback queries only.")
        print("          Drop a Performance Analyzer .json onto Resources for a real test.")
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

    resolved_name = _derive_load_test_name(load_test_name, boot.notebook_name)
    print(f"LoadTest  : {resolved_name}")

    cfg = RunConfig(
        load_test_name=resolved_name,
        load_test_description=load_test_description,
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

    rr = run_load_test(
        cfg, dotnet=boot.dotnet, loadgen_dll=boot.loadgen_dll,
        on_status=on_status)

    # Forensic artifacts (executions CSV, trace CSV, result.json, *.log)
    # stay on the Spark driver's local disk under run_local_dir. We
    # deliberately do NOT copy them to OneLake — everything the analytics
    # layer needs already lives in the 5 Delta tables. Run the load test
    # again and you'll get a fresh /tmp dir; if you need the raw artifacts
    # for forensics, grab them from `run_dest` before the kernel cycles.
    run_dest = rr.run_local_dir

    _print_run_banner(rr, run_dest)

    write_summary: Optional[WriteSummary] = None
    if rr.run_id:
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
            load_test_description=load_test_description,
            target_workspace=tgt_ws_name, target_dataset=ds.dataset_name,
            target_replica=target_replica, xmla=cfg.xmla,
            queries=queries, user_count=concurrent_users,
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
        print("  Tables     : LoadTests, LoadTestRuns, LoadTestQueries, "
              "QueryExecutions, TraceEvents")
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

    return RunOutcome(
        result=rr, write_summary=write_summary,
        load_test_name=resolved_name, cfg=cfg, run_dest=run_dest)


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
        lat = s.get("latency", {}) or {}
        print()
        print("=== Results ===")
        print(f"Total executions : {s.get('totalExecutions')}")
        print(f"Successful       : {s.get('successfulExecutions')}")
        print(f"Failed           : {s.get('failedExecutions')}")
        print(f"QPS              : {s.get('qps')}")
        if lat:
            print(f"Latency (ms)     : min={lat.get('min')}  "
                  f"median={lat.get('median')}  mean={lat.get('mean')}  "
                  f"p95={lat.get('p95')}  p99={lat.get('p99')}  "
                  f"max={lat.get('max')}")
        print(f"\nFull result      : {rr.result_envelope.get('resultFile')}")
        print(f"Run artifacts    : {run_dest}")


def analyze(outcome: RunOutcome) -> Any:
    """Render the latency/QPS/active-users figure for a finished run."""
    import matplotlib.pyplot as plt  # type: ignore

    from .analyze import plot_run

    fig = plot_run(
        outcome.result.csv_path,
        title=f"Run {outcome.result.run_id} — {outcome.load_test_name}",
        trace_csv_path=outcome.result.trace_csv_path,
    )
    plt.show()
    return fig
