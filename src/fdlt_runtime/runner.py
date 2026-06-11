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

    # Target
    target_workspace: str = "MyWorkspace"
    target_dataset: str = "My Semantic Model"
    target_replica: str = ""

    # Load shape
    duration_seconds: int = 60
    concurrent_users: int = 5
    concurrent_queries_per_user: int = 1
    pause_between_iterations_ms: int = 10000
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
        # Driver-CPU sampler. psutil is optional; if missing, the decorator
        # is a no-op and the envelope passes through unchanged.
        cpu_sampler = _DriverCpuSampler()
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
            if kind == "progress":
                cpu_sampler.decorate(env_obj)
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
    """Format a `progress` envelope into the one-line live-status string.

    Used for non-TTY callers (notebooks that overwrite a single line, CI
    log capture). For an interactive terminal experience prefer
    :class:`LiveDashboard`, which animates a multi-panel view.
    """
    parts = [
        f"[{env_obj.get('phase','?'):<10}]",
        f"elapsed={env_obj.get('elapsed',0):6.1f}s",
        f"users={env_obj.get('activeUsers',0)}/{env_obj.get('targetUsers',0)}",
        f"ok={env_obj.get('successful',0)}",
        f"err={env_obj.get('failed',0)}",
        f"qps={env_obj.get('qps',0):.1f}",
    ]
    inflight = env_obj.get("inFlight")
    if inflight is not None:
        parts.append(f"inflight={inflight}")
    p95 = env_obj.get("latencyMsP95")
    p99 = env_obj.get("latencyMsP99")
    if p95 is not None and (p95 or env_obj.get("latencySamples", 0)):
        parts.append(f"p95={p95:.0f}ms")
    if p99 is not None and (p99 or env_obj.get("latencySamples", 0)):
        parts.append(f"p99={p99:.0f}ms")
    cpu = env_obj.get("driverCpuPct")
    if cpu is not None:
        parts.append(f"cpu={cpu:.0f}%")
    mem = env_obj.get("driverMemPct")
    if mem is not None:
        parts.append(f"mem={mem:.0f}%")
    return "  ".join(parts)


# ---------------------------------------------------------------------------
# Driver-side CPU/MEM sampler (stdlib only)
# ---------------------------------------------------------------------------


class _DriverCpuSampler:
    """Decorate progress envelopes with driverCpuPct / driverMemPct fields.

    Stdlib-only. Linux/Mac uses /proc/stat (Spark driver case). Windows
    uses ctypes -> GetSystemTimes for a system-wide CPU% sample. Memory
    on Linux comes from /proc/meminfo. If anything fails, the fields are
    just omitted from the envelope — never a hard error.
    """

    def __init__(self) -> None:
        self._impl: Optional[Callable[[], Optional[float]]] = None
        self._mem: Optional[Callable[[], Optional[float]]] = None
        self._last_total: Optional[int] = None
        self._last_idle: Optional[int] = None
        self._win_last_idle: Optional[int] = None
        self._win_last_kernel: Optional[int] = None
        self._win_last_user: Optional[int] = None
        try:
            if os.path.exists("/proc/stat"):
                self._impl = self._linux_cpu
                self._impl()  # prime
                if os.path.exists("/proc/meminfo"):
                    self._mem = self._linux_mem
            elif os.name == "nt":
                self._impl = self._windows_cpu
                self._impl()  # prime
                self._mem = self._windows_mem
        except Exception:
            self._impl = None
            self._mem = None

    def decorate(self, env_obj: Dict[str, Any]) -> None:
        if self._impl is not None:
            try:
                v = self._impl()
                if v is not None:
                    env_obj.setdefault("driverCpuPct", float(v))
            except Exception:
                pass
        if self._mem is not None:
            try:
                v = self._mem()
                if v is not None:
                    env_obj.setdefault("driverMemPct", float(v))
            except Exception:
                pass

    def _linux_cpu(self) -> Optional[float]:
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()
        except OSError:
            return None
        if not line.startswith("cpu "):
            return None
        parts = line.split()
        # user nice system idle iowait irq softirq steal guest guest_nice
        nums = [int(x) for x in parts[1:]]
        idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
        total = sum(nums)
        if self._last_total is None:
            self._last_total, self._last_idle = total, idle
            return None
        d_total = total - self._last_total
        d_idle = idle - self._last_idle
        self._last_total, self._last_idle = total, idle
        if d_total <= 0:
            return 0.0
        return max(0.0, min(100.0, 100.0 * (1.0 - d_idle / d_total)))

    def _linux_mem(self) -> Optional[float]:
        try:
            info: Dict[str, int] = {}
            with open("/proc/meminfo", "r") as f:
                for ln in f:
                    k, _, rest = ln.partition(":")
                    val = rest.strip().split()
                    if val:
                        try:
                            info[k] = int(val[0])  # kB
                        except ValueError:
                            pass
            total = info.get("MemTotal", 0)
            avail = info.get("MemAvailable", info.get("MemFree", 0))
            if total <= 0:
                return None
            return 100.0 * (1.0 - avail / total)
        except OSError:
            return None

    def _windows_cpu(self) -> Optional[float]:
        try:
            import ctypes
            from ctypes import wintypes
            FILETIME = wintypes.FILETIME

            kernel32 = ctypes.windll.kernel32
            GetSystemTimes = kernel32.GetSystemTimes
            GetSystemTimes.restype = wintypes.BOOL
            GetSystemTimes.argtypes = [
                ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME)
            ]
            idle, krn, usr = FILETIME(), FILETIME(), FILETIME()
            if not GetSystemTimes(ctypes.byref(idle), ctypes.byref(krn), ctypes.byref(usr)):
                return None

            def _u64(ft: "wintypes.FILETIME") -> int:
                return (ft.dwHighDateTime << 32) | ft.dwLowDateTime

            i, k, u = _u64(idle), _u64(krn), _u64(usr)
            if (self._win_last_idle is None
                    or self._win_last_kernel is None
                    or self._win_last_user is None):
                self._win_last_idle = i
                self._win_last_kernel = k
                self._win_last_user = u
                return None
            d_idle = i - self._win_last_idle
            # Note: kernel time on Windows already INCLUDES idle.
            d_total = (k - self._win_last_kernel) + (u - self._win_last_user)
            self._win_last_idle = i
            self._win_last_kernel = k
            self._win_last_user = u
            if d_total <= 0:
                return 0.0
            return max(0.0, min(100.0, 100.0 * (1.0 - d_idle / d_total)))
        except Exception:
            return None

    def _windows_mem(self) -> Optional[float]:
        try:
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            ms = MEMORYSTATUSEX()
            ms.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(ms)):
                return None
            return float(ms.dwMemoryLoad)
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Stdlib live dashboard (ANSI escapes, no third-party deps)
# ---------------------------------------------------------------------------


# ANSI escape helpers. We avoid `colorama` and `rich` to honour the
# no-extra-deps constraint. Modern Windows Terminal and VS Code terminals
# render these natively; legacy cmd.exe falls through unscathed because the
# dashboard auto-detects non-TTY and falls back to a single-line writer.
_ANSI_RESET = "\x1b[0m"
_ANSI_HIDE = "\x1b[?25l"
_ANSI_SHOW = "\x1b[?25h"
_ANSI_CLEAR_LINE = "\x1b[2K"


def _ansi(*codes: int) -> str:
    return "\x1b[" + ";".join(str(c) for c in codes) + "m"


class LiveDashboard:
    """Animated terminal dashboard fed by progress envelopes.

    Usage::

        with LiveDashboard() as dash:
            run_load_test(cfg, on_status=dash.update, ...)

    Auto-detects TTY; on a non-interactive stream (CI, notebook, file
    redirect) falls back to the one-line :func:`render_progress` writer
    so the same code path works everywhere. Stdlib-only — no rich/psutil.
    """

    _HISTORY = 60  # samples kept for sparklines (~1 minute at 1Hz)
    _SPARK = "▁▂▃▄▅▆▇█"

    def __init__(self, *, force_text: bool = False) -> None:
        self._qps_hist: deque[float] = deque(maxlen=self._HISTORY)
        self._p95_hist: deque[float] = deque(maxlen=self._HISTORY)
        self._cpu_hist: deque[float] = deque(maxlen=self._HISTORY)
        self._peak_qps: float = 0.0
        self._last_text: str = ""
        self._lines_drawn: int = 0
        try:
            import sys as _sys
            self._stdout = _sys.stdout
            self._tty = (not force_text) and bool(getattr(self._stdout, "isatty", lambda: False)())
        except Exception:
            self._tty = False
        # Try to enable VT processing on Windows so ANSI escapes render.
        if self._tty and os.name == "nt":
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
                mode = ctypes.c_ulong()
                if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                    # ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
                    kernel32.SetConsoleMode(handle, mode.value | 0x0004)
            except Exception:
                pass

    def __enter__(self) -> "LiveDashboard":
        if self._tty:
            try:
                self._stdout.write(_ANSI_HIDE)
                self._stdout.flush()
            except Exception:
                pass
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._tty:
            try:
                self._stdout.write(_ANSI_SHOW + "\n")
                self._stdout.flush()
            except Exception:
                pass
        else:
            try:
                self._stdout.write("\n")
                self._stdout.flush()
            except Exception:
                pass

    def update(self, env_obj: Mapping[str, Any]) -> None:
        kind = env_obj.get("type")
        if kind != "progress":
            return
        try:
            qps = float(env_obj.get("qps") or 0.0)
            p95 = float(env_obj.get("latencyMsP95") or 0.0)
            cpu = float(env_obj.get("driverCpuPct") or 0.0)
        except (TypeError, ValueError):
            qps, p95, cpu = 0.0, 0.0, 0.0
        self._qps_hist.append(qps)
        self._p95_hist.append(p95)
        self._cpu_hist.append(cpu)
        if qps > self._peak_qps:
            self._peak_qps = qps
        if self._tty:
            self._render_panel(env_obj)
        else:
            self._render_text(env_obj)

    # ---- helpers ----

    @classmethod
    def _spark(cls, values: Sequence[float]) -> str:
        if not values:
            return ""
        lo = min(values)
        hi = max(values)
        if hi <= lo:
            return cls._SPARK[0] * len(values)
        scale = (len(cls._SPARK) - 1) / (hi - lo)
        out = []
        for v in values:
            idx = int((v - lo) * scale)
            if idx < 0:
                idx = 0
            elif idx >= len(cls._SPARK):
                idx = len(cls._SPARK) - 1
            out.append(cls._SPARK[idx])
        return "".join(out)

    @staticmethod
    def _phase_color(phase: str) -> int:
        # ANSI foreground codes.
        return {
            "Pending": 90,      # bright black / grey
            "Connecting": 33,   # yellow
            "Steady": 32,       # green
            "Cancelling": 35,   # magenta
            "Done": 36,         # cyan
            "Cancelled": 35,    # magenta
            "Failed": 31,       # red
        }.get(phase, 37)

    @staticmethod
    def _bar(ratio: float, width: int = 20) -> str:
        ratio = 0.0 if ratio < 0 else (1.0 if ratio > 1 else ratio)
        fill = int(round(ratio * width))
        return "█" * fill + "░" * (width - fill)

    def _render_panel(self, env_obj: Mapping[str, Any]) -> None:
        phase = str(env_obj.get("phase", "Pending"))
        color = self._phase_color(phase)

        elapsed = float(env_obj.get("elapsed", 0) or 0)
        active = int(env_obj.get("activeUsers", 0) or 0)
        target = int(env_obj.get("targetUsers", 0) or 0)
        ok = int(env_obj.get("successful", 0) or 0)
        err = int(env_obj.get("failed", 0) or 0)
        qps = float(env_obj.get("qps", 0) or 0)
        inflight = env_obj.get("inFlight")
        p50 = float(env_obj.get("latencyMsP50") or 0)
        p95 = float(env_obj.get("latencyMsP95") or 0)
        p99 = float(env_obj.get("latencyMsP99") or 0)
        latn = int(env_obj.get("latencySamples") or 0)
        cpu = env_obj.get("driverCpuPct")
        mem = env_obj.get("driverMemPct")
        err_rate = (err / (ok + err) * 100.0) if (ok + err) else 0.0

        bold = _ansi(1)
        reset = _ANSI_RESET
        col = _ansi(color)
        col_bold = _ansi(1, color)
        green = _ansi(32)
        red = _ansi(31)
        yellow = _ansi(33)
        cyan = _ansi(36)

        ratio = (active / target) if target > 0 else 0.0
        lines: List[str] = []
        # Header
        lines.append(
            f"{_ansi(7, color)} {phase:<10} {reset}"
            f"  {bold}elapsed{reset} {elapsed:6.1f}s"
            f"  {bold}users{reset} {active:>3}/{target:<3}  {col}{self._bar(ratio)}{reset}"
        )
        # QPS line
        lines.append(
            f"  {bold}QPS{reset}        {green}{bold}{qps:7.1f}{reset}"
            f"   peak {self._peak_qps:6.1f}   {cyan}{self._spark(self._qps_hist)}{reset}"
        )
        # OK / ERR line
        err_seg = f"{red}{err}{reset}" if err else f"{err}"
        lines.append(
            f"  {bold}OK / ERR{reset}   {green}{ok}{reset} / {err_seg}"
            f"   ({err_rate:.2f}% errors)"
        )
        # In-flight
        if inflight is not None:
            lines.append(f"  {bold}In-flight{reset}  {inflight}")
        # Latency line
        lines.append(
            f"  {bold}Latency ms{reset} "
            f"p50 {bold}{p50:6.0f}{reset}   "
            f"p95 {yellow}{bold}{p95:6.0f}{reset}   "
            f"p99 {red}{bold}{p99:6.0f}{reset}   "
            f"n={latn}   {cyan}{self._spark(self._p95_hist)}{reset}"
        )
        # Driver
        if cpu is not None:
            mem_str = f"   mem {float(mem):5.1f}%" if mem is not None else ""
            cpu_color = _ansi(31) if float(cpu) >= 90 else (_ansi(33) if float(cpu) >= 70 else _ansi(32))
            lines.append(
                f"  {bold}Driver{reset}     "
                f"cpu {cpu_color}{bold}{float(cpu):5.1f}%{reset}{mem_str}"
                f"   {cyan}{self._spark(self._cpu_hist)}{reset}"
            )

        # Move cursor to top of previous frame, redraw, clear leftover lines.
        out: List[str] = []
        if self._lines_drawn:
            out.append(f"\x1b[{self._lines_drawn}F")  # cursor up + col 1
        for ln in lines:
            out.append(_ANSI_CLEAR_LINE + ln + "\n")
        # If previous frame had more lines, blank the trailing ones.
        for _ in range(max(0, self._lines_drawn - len(lines))):
            out.append(_ANSI_CLEAR_LINE + "\n")
        try:
            self._stdout.write("".join(out))
            self._stdout.flush()
        except Exception:
            pass
        self._lines_drawn = max(self._lines_drawn, len(lines))

    def _render_text(self, env_obj: Mapping[str, Any]) -> None:
        line = render_progress(env_obj)
        pad = max(0, len(self._last_text) - len(line))
        try:
            self._stdout.write("\r" + line + (" " * pad))
            self._stdout.flush()
        except Exception:
            pass
        self._last_text = line
