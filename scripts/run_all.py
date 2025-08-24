# run_all.py — robust supervisor for sidecar(s) and optional bot
import os, sys, time, pathlib, subprocess, datetime as _dt, threading, signal, argparse

# Unbuffered output
os.environ.setdefault("PYTHONUNBUFFERED", "1")

ROOT = pathlib.Path(__file__).resolve().parent
PY = sys.executable
ENV = os.environ.copy()

# Load env/ .env if python-dotenv present
try:
    from dotenv import load_dotenv
    for fname in ("env", ".env"):
        f = ROOT / fname
        if f.exists():
            load_dotenv(f)
            print(f"[ENV] loaded {fname}")
            break
except Exception:
    pass

LOGS = ROOT / "logs"
LOGS.mkdir(parents=True, exist_ok=True)

def _open_log(name: str):
    return open(LOGS / name, "a", buffering=1, encoding="utf-8")

def _start_sidecar_heartbeat(period_sec: int = 30):
    """Periodic heartbeat so !status shows activity even if no events flow"""
    beat_path = LOGS / "beat_sidecar.txt"
    def _tick():
        while True:
            try:
                beat_path.write_text(_dt.datetime.now(_dt.timezone.utc).isoformat(), encoding="utf-8")
            except Exception:
                pass
            time.sleep(max(5, int(period_sec)))
    threading.Thread(target=_tick, name="SidecarHeartbeat", daemon=True).start()

class ProcSpec:
    def __init__(self, name, argv, log_name, cwd=None):
        self.name = name
        self.argv = argv
        self.log_name = log_name
        self.cwd = cwd or str(ROOT)
        self.restarts = 0

def _spawn(spec: ProcSpec):
    log = _open_log(spec.log_name)
    print(f"[SPAWN] {spec.name}: {spec.argv}")
    return subprocess.Popen(spec.argv, cwd=spec.cwd, stdout=log, stderr=subprocess.STDOUT)

def _supervise(spec: ProcSpec, max_restarts: int = 3, base_backoff: float = 1.0):
    p = _spawn(spec)
    while True:
        rc = p.wait()
        print(f"[EXIT ] {spec.name}: code={rc}")
        if rc == 0:
            return 0
        if spec.restarts >= max_restarts:
            print(f"[STOP ] {spec.name}: max restarts reached ({spec.restarts})")
            return rc
        spec.restarts += 1
        delay = min(30.0, base_backoff * (2 ** (spec.restarts - 1)))
        print(f"[RETRY] {spec.name}: restarting in {delay:.1f}s (attempt {spec.restarts}/{max_restarts})")
        time.sleep(delay)
        p = _spawn(spec)

def main():
    ap = argparse.ArgumentParser(description="Supervisor for logger and optional bot")
    ap.add_argument("--with-bot", action="store_true", help="Also launch TradingBot.py in MODE from env")
    ap.add_argument("--mode", default=os.getenv("MODE", "BACKTEST"), help="Override MODE for bot (BACKTEST/PAPER/LIVE)")
    ap.add_argument("--max-restarts", type=int, default=3, help="Max restarts per process")
    ap.add_argument("--heartbeat", type=int, default=30, help="Sidecar heartbeat seconds")
    args = ap.parse_args()

    _start_sidecar_heartbeat(args.heartbeat)

    specs = []
    if (ROOT / "logger_service.py").exists():
        specs.append(ProcSpec("logger_service", [PY, "logger_service.py"], "logger.out"))
    else:
        print("[INFO ] logger_service.py not found; skipping")

    if args.with_bot and (ROOT / "TradingBot.py").exists():
        env_mode = args.mode.upper()
        os.environ["MODE"] = env_mode
        specs.append(ProcSpec(f"TradingBot[{env_mode}]", [PY, "TradingBot.py"], f"bot_{env_mode.lower()}.out"))
    elif args.with_bot:
        print("[WARN ] TradingBot.py not found; --with-bot ignored")

    # Graceful shutdown
    stopping = {"flag": False}
    def _handle_sig(signum, frame):
        print(f"[SIGNL] received {signum}; stopping children...")
        stopping["flag"] = True
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_sig)
        except Exception:
            pass

    # Start each spec in its own supervising thread
    def _runner(spec: ProcSpec):
        _supervise(spec, max_restarts=args.max_restarts)

    threads = []
    for s in specs:
        t = threading.Thread(target=_runner, args=(s,), daemon=True, name=f"supervise:{s.name}")
        t.start()
        threads.append(t)

    try:
        while not stopping["flag"]:
            alive = any(t.is_alive() for t in threads) or not threads
            if not alive:
                break
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        print("[DONE ] supervisor exiting")

if __name__ == "__main__":
    main()
