"""Out-of-process LoadGen runner.

Pulled from notebook cell 4. Spawns `dotnet LoadGen.dll --json-progress`,
parses JSONL envelopes from stdout, surfaces live progress via an
`on_status` callback, and returns a RunResult with everything cell 5 /
5b need to persist.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence


@dataclass
class RunConfig:
    """Inputs to a single LoadGen run.

    Mirrors the cell-1 parameter block plus the artifacts cell 3 used to
    materialize on disk. The notebook still owns scenario loading
    (via `fdlt_runtime.load_queries`) and token acquisition; everything
    that talks to LoadGen lives here.
    """

    # Identity / human labels
    load_test_name: str = "my-load-test"
    load_test_description: str = ""

    # Target
    target_workspace: str = "MyWorkspace"
    target_dataset: str = "My Semantic Model"
    target_replica: str = ""

    # Load shape
    duration_seconds: int = 60
    concurrent_users: int = 5
    concurrent_queries_per_user: int = 1
    pause_between_iterations_ms: int = 1000
    pause_between_queries_ms: int = 0
    user_ramp_time_sec: int = 15
    skip_results: bool = False

    # When False, pass --no-trace so LoadGen skips the XMLA trace
    # subscription. Default True preserves the engine-telemetry capture.
    enable_tracing: bool = True

    # Materialized inputs
    queries: Sequence[str] = field(default_factory=list)
    users: Sequence[Mapping[str, str]] = field(default_factory=list)

    # Auth
    token: str = ""

    @property
    def xmla(self) -> str:
        return f"powerbi://api.powerbi.com/v1.0/myorg/{self.target_workspace}"


@dataclass
class RunResult:
    """Outputs of a single LoadGen run.

    The notebook reads `csv_path` to build the per-query DataFrame and
    feeds `result_envelope` / `error_envelope` / `returncode` into the
    LoadTestRuns row for the run.
    """

    run_id: Optional[str]
    staging_id: str
    run_local_dir: str
    csv_path: str
    trace_csv_path: str
    log_file: str
    returncode: int
    started_at: datetime
    ended_at: datetime
    result_envelope: Optional[Dict[str, Any]]
    error_envelope: Optional[Dict[str, Any]]
    stderr_tail: List[str]


def run_load_test(
    cfg: RunConfig,
    *,
    dotnet: str,
    loadgen_dll: str,
    on_status: Optional[Callable[[Dict[str, Any]], None]] = None,
    log_folder: Optional[str] = None,
) -> RunResult:
    """Launch LoadGen, stream JSONL progress, and return a RunResult.

    `on_status` is called for every envelope (progress / started / result /
    error / unknown). The notebook's live status line is just the
    progress envelope formatted; this lets cell 3 own the IPython
    `update_display` calls without burying the formatting in here.

    `log_folder` controls where the LoadGen subprocess writes its
    artifacts (executions CSV, trace CSV, *.log). When None (default),
    a fresh ``/tmp/fdlt-run-<id>`` dir is used — driver-local, fast,
    and discarded when the kernel cycles. Pass a local path
    (e.g. ``/lakehouse/default/Files/loadtest-logs``) to write
    directly to that location — useful when you want artifacts to land
    in OneLake live during the run rather than via the post-run copy.
    ``abfss://`` URLs are NOT supported here (the .NET process can't
    write to OneLake directly); the caller should leave ``log_folder``
    None and use a notebookutils-based copy after the run instead.

    Cancellation: a `KeyboardInterrupt` raised by the caller (Spark
    "Interrupt Kernel") is forwarded to the child as SIGINT; LoadGen
    drains and exits with code 130.
    """
    staging_id = uuid.uuid4().hex[:8]
    if log_folder is None:
        run_local = f"/tmp/fdlt-run-{staging_id}"
    else:
        if log_folder.startswith("abfss://"):
            raise ValueError(
                "run_load_test() log_folder must be a local path. "
                "abfss:// destinations are post-run copies handled by "
                "notebook.py::_persist_run_logs.")
        run_local = os.path.join(log_folder, f"fdlt-run-{staging_id}")
    os.makedirs(run_local, exist_ok=True)
    log_file = (
        f"LoadTest.{cfg.concurrent_users}u."
        f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.csv"
    )

    queries_json = os.path.join(run_local, "queries.json")
    users_json = os.path.join(run_local, "users.json")
    with open(queries_json, "w", encoding="utf-8") as f:
        json.dump(list(cfg.queries), f)
    with open(users_json, "w", encoding="utf-8") as f:
        # cfg.users has already been through normalize_users, which produces
        # canonical v2 keys. Write them straight through.
        def _user_to_v2(u: dict) -> dict:
            return {
                "effectiveUserName": u.get("effectiveUserName", ""),
                "customData": u.get("customData", ""),
                "roles": u.get("roles", ""),
            }
        json.dump([_user_to_v2(u) for u in cfg.users], f)

    cmd = [
        dotnet, loadgen_dll, "--json-progress",
        "--xmla", cfg.xmla,
        "--dataset", cfg.target_dataset,
        "--duration", str(cfg.duration_seconds),
        "--users", str(cfg.concurrent_users),
        "--concurrent-queries-per-user", str(cfg.concurrent_queries_per_user),
        "--pause-iterations", str(cfg.pause_between_iterations_ms),
        "--pause-queries", str(cfg.pause_between_queries_ms),
        "--ramp-time", str(cfg.user_ramp_time_sec),
        "--queries-file", queries_json,
        "--users-file", users_json,
        "--log-dir", run_local,
        "--log-file", log_file,
    ]
    if cfg.target_replica:
        cmd += ["--replica", cfg.target_replica]
    if cfg.skip_results:
        cmd += ["--skip-results"]
    if not cfg.enable_tracing:
        cmd += ["--no-trace"]

    # Token via env, NOT argv: process listings on shared compute would
    # otherwise expose the bearer token.
    env = {**os.environ, "PBI_TOKEN": cfg.token}

    started_at = datetime.now(timezone.utc)
    proc = subprocess.Popen(
        cmd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )
    stderr_buf: "deque[str]" = deque(maxlen=1000)

    def _drain_stderr() -> None:
        assert proc.stderr is not None
        for line in proc.stderr:
            stderr_buf.append(line.rstrip("\n"))

    threading.Thread(target=_drain_stderr, daemon=True).start()

    run_id: Optional[str] = None
    result_envelope: Optional[Dict[str, Any]] = None
    error_envelope: Optional[Dict[str, Any]] = None

    try:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.strip()
            if not line:
                continue
            try:
                env_obj = json.loads(line)
            except json.JSONDecodeError:
                env_obj = {"type": "unknown", "raw": line}
            kind = env_obj.get("type")
            if kind == "started" and env_obj.get("runId"):
                run_id = env_obj["runId"]
            elif kind == "result":
                result_envelope = env_obj
            elif kind == "error":
                error_envelope = env_obj
            if on_status is not None:
                try:
                    on_status(env_obj)
                except Exception:
                    pass  # caller-side rendering must not kill the run
    except KeyboardInterrupt:
        try:
            proc.send_signal(signal.SIGINT)
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    finally:
        proc.wait()

    ended_at = datetime.now(timezone.utc)

    return RunResult(
        run_id=run_id,
        staging_id=staging_id,
        run_local_dir=run_local,
        csv_path=os.path.join(run_local, log_file),
        trace_csv_path=os.path.join(
            run_local,
            (os.path.splitext(log_file)[0] if log_file else "LoadTest")
            + ".trace.csv"),
        log_file=log_file,
        returncode=proc.returncode,
        started_at=started_at,
        ended_at=ended_at,
        result_envelope=result_envelope,
        error_envelope=error_envelope,
        stderr_tail=list(stderr_buf),
    )


def render_progress(env_obj: Mapping[str, Any]) -> str:
    """Format a `progress` envelope into the one-line live-status string."""
    return (
        f"[{env_obj.get('phase','?'):<10}] "
        f"elapsed={env_obj.get('elapsed',0):6.1f}s  "
        f"users={env_obj.get('activeUsers',0)}/{env_obj.get('targetUsers',0)}  "
        f"ok={env_obj.get('successful',0)}  err={env_obj.get('failed',0)}  "
        f"qps={env_obj.get('qps',0):.1f}"
    )
