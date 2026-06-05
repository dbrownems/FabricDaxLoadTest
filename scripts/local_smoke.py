"""Local subprocess smoke test for LoadGen.

Mimics the notebook's cell-4 subprocess flow without needing Livy.
Runs: dotnet LoadGen.dll --json-progress ... and parses JSONL stdout.

Usage:
    python scripts\local_smoke.py [--bad-token]

Reads workspace + dataset from CLI flags or these defaults
(dbrowne-loadtest workspace, DIAD model from prior smoke runs).
"""
import argparse, json, os, signal, subprocess, sys, threading, time, uuid
from collections import deque
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PUBLISH = REPO / "src" / "LoadGen" / "bin" / "Release" / "net8.0" / "win-x64" / "publish"
LOADGEN_DLL = PUBLISH / "LoadGen.dll"


def get_token() -> str:
    out = subprocess.check_output(
        ["az", "account", "get-access-token",
         "--resource", "https://analysis.windows.net/powerbi/api",
         "--query", "accessToken", "-o", "tsv"],
        text=True, shell=True,
    )
    return out.strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", default="dbrowne-loadtest")
    ap.add_argument("--dataset",   default="DIAD Final Report with RLS")
    ap.add_argument("--users",     type=int, default=3)
    ap.add_argument("--duration",  type=int, default=15)
    ap.add_argument("--ramp",      type=int, default=2)
    ap.add_argument("--bad-token", action="store_true",
                    help="Use an obviously bad token to force failure path")
    ap.add_argument("--no-token", action="store_true",
                    help="Local SSAS / PBI Desktop — Windows auth, no bearer token")
    ap.add_argument("--xmla", default=None,
                    help="Override XMLA endpoint (e.g. localhost:2383 for local SSAS)")
    args = ap.parse_args()

    if not LOADGEN_DLL.exists():
        print(f"ERROR: LoadGen.dll not found at {LOADGEN_DLL}", file=sys.stderr)
        print("Run: dotnet publish src/LoadGen/LoadGen.csproj -c Release -r win-x64 "
              "-p:SelfContained=false -p:PublishSingleFile=false -p:UseAppHost=false",
              file=sys.stderr)
        return 2

    run_id = uuid.uuid4().hex[:8]
    run_dir = REPO / "tmp" / f"fdlt-run-{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    queries_json = run_dir / "queries.json"
    users_json   = run_dir / "users.json"
    # For local SSAS connections we don't have a real upn/role — pass empty
    # to bypass EffectiveUserName / Roles clauses.
    if args.no_token:
        users_payload = [{"email": "", "role": ""}]
    else:
        users_payload = [{"email": "anonymous@local", "role": ""}]
    queries_json.write_text(json.dumps(['EVALUATE ROW("x", 1)']), encoding="utf-8")
    users_json.write_text(json.dumps(users_payload), encoding="utf-8")

    token = "bad-token-deliberately-invalid" if args.bad_token else (
        "" if args.no_token else get_token())
    xmla  = args.xmla or f"powerbi://api.powerbi.com/v1.0/myorg/{args.workspace}"

    cmd = [
        "dotnet", str(LOADGEN_DLL), "--json-progress",
        "--xmla", xmla,
        "--dataset", args.dataset,
        "--duration", str(args.duration),
        "--users", str(args.users),
        "--queries-per-batch", "1",
        "--pause-iterations", "100",
        "--pause-queries", "0",
        "--ramp-time", str(args.ramp),
        "--queries-file", str(queries_json),
        "--users-file", str(users_json),
        "--log-dir", str(run_dir),
        "--log-file", f"LoadTest.{args.users}u.csv",
    ]

    if args.no_token:
        cmd.append("--no-auth")
    if not args.no_token:
        env_token = "bad-token-deliberately-invalid" if args.bad_token else token
    else:
        env_token = ""

    env = {**os.environ}
    if env_token:
        env["PBI_TOKEN"] = env_token
    print(f"Run     : {run_id}  (bad-token={args.bad_token}, no-token={args.no_token})")
    print(f"Cmd     : {' '.join(cmd[:3])} ... ({len(cmd)} args)")
    print(f"Run dir : {run_dir}")
    print()

    proc = subprocess.Popen(
        cmd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )

    stderr_buf: deque[str] = deque(maxlen=1000)
    def _drain():
        for line in proc.stderr:
            stderr_buf.append(line.rstrip("\n"))
    threading.Thread(target=_drain, daemon=True).start()

    result_envelope = None
    error_envelope  = None
    n_progress = 0
    last_status = ""

    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                env_obj = json.loads(line)
            except json.JSONDecodeError:
                print(f"(non-JSON stdout) {line}")
                continue
            kind = env_obj.get("type")
            if kind == "started":
                print(f"[started] {json.dumps(env_obj)[:200]}")
            elif kind == "progress":
                n_progress += 1
                status = (
                    f"[{env_obj.get('phase','?'):<10}] "
                    f"elapsed={env_obj.get('elapsed',0):6.1f}s  "
                    f"users={env_obj.get('activeUsers',0)}/{env_obj.get('targetUsers',0)}  "
                    f"ok={env_obj.get('successful',0)}  err={env_obj.get('failed',0)}  "
                    f"qps={env_obj.get('qps',0):.1f}"
                )
                # Overwrite previous line for live feel.
                sys.stdout.write("\r" + status.ljust(len(last_status)))
                sys.stdout.flush()
                last_status = status
            elif kind == "result":
                result_envelope = env_obj
                print()
                print(f"[result ] resultFile={env_obj.get('resultFile')}")
            elif kind == "error":
                error_envelope = env_obj
                print()
                print(f"[error  ] code={env_obj.get('code')} type={env_obj.get('exceptionType')}")
            else:
                print(f"[?{kind}] {json.dumps(env_obj)[:200]}")
    except KeyboardInterrupt:
        print("\nSIGINT — forwarding to child")
        try: proc.send_signal(signal.SIGINT)
        except Exception: pass
        try: proc.wait(timeout=30)
        except subprocess.TimeoutExpired: proc.terminate()
    finally:
        proc.wait()

    print()
    print(f"exit code      : {proc.returncode}")
    print(f"progress msgs  : {n_progress}")
    print(f"got result?    : {result_envelope is not None}")
    print(f"got error?     : {error_envelope is not None}")

    if error_envelope is not None or proc.returncode not in (0, 130):
        print()
        print("=== FAILURE DETAIL ===")
        if error_envelope:
            print(f"code   : {error_envelope.get('code')}")
            print(f"type   : {error_envelope.get('exceptionType')}")
            print(f"message: {error_envelope.get('message')}")
            exc = error_envelope.get("exception", "")
            if exc:
                print("--- exception (first 80 lines) ---")
                for ln in exc.splitlines()[:80]:
                    print(ln)
        if stderr_buf:
            print("--- stderr tail (last 30) ---")
            for ln in list(stderr_buf)[-30:]:
                print(ln)
        return 1

    if result_envelope is not None:
        s = result_envelope.get("summary", {}) or {}
        print()
        print("=== Results ===")
        print(f"total : {s.get('totalExecutions')}  ok={s.get('successfulExecutions')}  "
              f"err={s.get('failedExecutions')}  qps={s.get('qps')}")
        lat = s.get("latency", {}) or {}
        if lat:
            print(f"latency ms: min={lat.get('min')} median={lat.get('median')} "
                  f"mean={lat.get('mean')} p95={lat.get('p95')} max={lat.get('max')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
