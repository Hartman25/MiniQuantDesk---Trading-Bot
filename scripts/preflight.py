
import os
import sys
import json
import time
import uuid
import subprocess
from pathlib import Path
from datetime import datetime, timezone
import argparse

# ---------- Core paths ----------
ROOT = Path(__file__).resolve().parent
LOG_DIR = Path(os.getenv("LOG_DIR", str(ROOT / "ml_sidecar" / "logs")))
DATA_DIR = Path(os.getenv("DATA_DIR", str(ROOT / "ml_sidecar" / "data" / "ml")))

# ---------- dotenv (optional) ----------
try:
    from dotenv import load_dotenv
    _HAS_DOTENV = True
except Exception:
    _HAS_DOTENV = False

if _HAS_DOTENV:
    loaded_env = False
    for fname in ("env", ".env"):  # prefer 'env' then '.env'
        env_path = ROOT / fname
        if env_path.exists():
            load_dotenv(env_path)
            print(f"[ENV] loaded {fname} from {ROOT}")
            loaded_env = True
            break
    if not loaded_env:
        print("[ENV] no .env or env file found, relying on process env")
else:
    print("[ENV] python-dotenv not installed; relying on process env")

# ---------- requests (optional, used for webhooks/kill URL) ----------
try:
    import requests
    _HAS_REQUESTS = True
except Exception:
    _HAS_REQUESTS = False

# ---------- Alpaca (optional) ----------
_ALPACA_ERR = None
try:
    try:
        from alpaca.trading.client import TradingClient as _TradingClient  # type: ignore
        _ALPACA_VER = "alpaca-py"
    except Exception:
        _TradingClient = None
        _ALPACA_VER = None
except Exception as e:
    _ALPACA_ERR = e
    _TradingClient = None
    _ALPACA_VER = None

# ---------- Sidecar bus (optional) ----------
try:
    from ml_sidecar import event_bus as _event_bus
    _HAS_BUS = True
except Exception:
    _HAS_BUS = False

def _log_path(name: str) -> Path:
    return LOG_DIR / name

def _print(line: str):
    sys.stdout.write(line.rstrip() + "\n")
    sys.stdout.flush()

def _pass(msg: str): _print(msg)
def _fail(msg: str): _print(msg)
def _warn(msg: str): _print(msg)

def check_logdir() -> bool:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _fail(f"[LOG_DIR] FAIL: cannot create {LOG_DIR} ({e})")
        return False

    probe = _log_path(f"_preflight_probe_{uuid.uuid4().hex}.txt")
    try:
        probe.write_text("ok", encoding="utf-8")
        _ = probe.read_text(encoding="utf-8")
        try:
            probe.unlink(missing_ok=True)
        except Exception:
            pass
        _pass(f"[LOG_DIR] PASS: writable at {LOG_DIR}")
        return True
    except Exception as e:
        _fail(f"[LOG_DIR] FAIL: not writable at {LOG_DIR} ({e})")
        return False

def check_backtest_params() -> bool:
    ok = True
    interval = os.getenv("BACKTEST_INTERVAL", "1Day").strip()
    lookback = int(os.getenv("BACKTEST_LOOKBACK_DAYS", "120") or "120")
    passes = int(os.getenv("BACKTEST_PASSES", "3") or "3")

    if interval not in ("1Day", "1Min"):
        _warn(f"[BACKTEST] WARN: Unknown interval '{interval}', expected 1Day or 1Min")
    if lookback <= 0:
        _fail(f"[BACKTEST] FAIL: BACKTEST_LOOKBACK_DAYS must be > 0 (got {lookback})"); ok = False
    if passes <= 0:
        _fail(f"[BACKTEST] FAIL: BACKTEST_PASSES must be > 0 (got {passes})"); ok = False
    if ok:
        _pass(f"[BACKTEST] PASS: Interval={interval}, Lookback={lookback}, Passes={passes}")
    return ok

def _alpaca_check(kind: str) -> bool:
    try:
        if _ALPACA_VER != "alpaca-py" or _TradingClient is None:
            _warn(f"[{kind}] WARN: alpaca-py not available; skipping {kind} account/clock checks")
            return True
        key = os.getenv(f"APCA_API_KEY_ID_{kind}")
        sec = os.getenv(f"APCA_API_SECRET_KEY_{kind}")
        base = os.getenv(f"APCA_{kind}_BASE_URL")
        if not key or not sec:
            _warn(f"[{kind}] WARN: missing keys; skipping")
            return True

        trading = _TradingClient(api_key=key, secret_key=sec, paper=(kind=="PAPER"))
        account = trading.get_account()
        status = getattr(account, "status", "unknown")
        equity = getattr(account, "equity", "0")
        _pass(f"[{kind}] PASS: account status={status}, equity={equity}")
        try:
            clock = trading.get_clock()
            is_open = getattr(clock, "is_open", False)
            _pass(f"[{kind}] PASS: clock ok; market_open={is_open}")
        except Exception as ce:
            _fail(f"[{kind}] FAIL: clock error: {ce}")
            return False
        return True
    except Exception as e:
        _fail(f"[{kind}] FAIL: {e}")
        return False

def check_alpaca() -> bool:
    ok_p = _alpaca_check("PAPER")
    ok_l = _alpaca_check("LIVE")
    return ok_p and ok_l

def check_smoke_backtest(timeout_sec: int = 120) -> bool:
    bot = ROOT / "TradingBot.py"
    if not bot.exists():
        _warn("[SMOKE] WARN: TradingBot.py not found; skipping smoke backtest")
        return True

    env = os.environ.copy()
    env["MODE"] = "BACKTEST"
    env["BACKTEST_LOOKBACK_DAYS"] = os.getenv("BACKTEST_LOOKBACK_DAYS", "5")
    env["BACKTEST_PASSES"] = os.getenv("BACKTEST_PASSES", "1")
    env["SYMBOLS"] = os.getenv("SYMBOLS", "SPY")

    try:
        p = subprocess.Popen([sys.executable, str(bot)], cwd=str(ROOT), env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            outs, errs = p.communicate(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            p.kill()
            _fail("[SMOKE] FAIL: backtest timed out")
            return False
        if p.returncode != 0:
            _warn(f"[SMOKE] WARN: backtest exited with code {p.returncode}")
        eq = _log_path("backtest_equity_curve.csv")
        rep = _log_path("backtest_report.csv")
        summ = _log_path("backtest_report_summary.csv")
        _print("== Backtest Smoke (LOG_DIR) ==")
        _print(f"  equity_curve.csv       exists={eq.exists()} size={eq.stat().st_size if eq.exists() else 0}")
        _print(f"  report.csv             exists={rep.exists()} size={rep.stat().st_size if rep.exists() else 0}")
        _print(f"  report_summary.csv     exists={summ.exists()} size={summ.stat().st_size if summ.exists() else 0}")
        ok = eq.exists() and rep.exists() and summ.exists()
        if ok:
            _pass("[SMOKE] PASS: artifacts present")
        else:
            _fail("[SMOKE] FAIL: missing artifacts")
        return ok
    except Exception as e:
        _fail(f"[SMOKE] FAIL: error launching backtest: {e}")
        return False

def check_sidecar_and_parquet(wait_secs: float = 3.0) -> bool:
    """Publish a preflight_ping and verify sidecar heartbeat + parquet write."""
    # Ensure project root is on sys.path and import bus lazily
    try:
        import sys as _sys
        if str(ROOT) not in _sys.path:
            _sys.path.insert(0, str(ROOT))
        try:
            from ml_sidecar.sidecar import event_bus as _event_bus  # your layout
        except Exception:
            from ml_sidecar import event_bus as _event_bus          # fallback
    except Exception as e:
        _warn(f"[SIDECAR] WARN: event bus not importable ({e}); skipping sidecar checks")
        return True

    # Heartbeat file path
    beat = _log_path("beat_sidecar.txt")
    before = beat.stat().st_mtime if beat.exists() else 0.0

    # Publish a preflight ping
    try:
        payload = {
            "event": "preflight_ping",
            "ts": datetime.now(timezone.utc).isoformat(),
            "session": datetime.now().date().isoformat(),
            "note": "preflight sanity ping",
        }
        _event_bus.bus.send(payload)
    except Exception as e:
        _fail(f"[SIDECAR] FAIL: unable to publish preflight_ping: {e}")
        return False

    # Wait for heartbeat mtime to update
    end = time.time() + wait_secs
    updated = False
    while time.time() < end:
        time.sleep(0.25)
        if beat.exists() and beat.stat().st_mtime > before:
            updated = True
            break
    if updated:
        _pass(f"[SIDECAR] PASS: heartbeat updated at {beat}")
    else:
        _warn(f"[SIDECAR] WARN: no heartbeat update at {beat} — is logger_service.py running?")

    # Parquet existence check for today's preflight_ping
    today = datetime.now().date()
    pq = DATA_DIR / "preflight_ping" / f"{today.year:04d}" / f"{today.month:02d}" / f"{today.day:02d}" / f"preflight_ping_{today.isoformat()}.parquet"
    if pq.exists():
        try:
            size = pq.stat().st_size
        except Exception:
            size = -1
        _pass(f"[PARQUET] PASS: preflight_ping written (file={pq}, size={size})")
        return updated
    else:
        _warn(f"[PARQUET] WARN: preflight_ping parquet not found at {pq}")
        return updated

def check_kill_controls(offline: bool = False) -> bool:
    ok = True
    kill_file = (os.getenv("KILL_FILE") or "").strip()
    kill_url = (os.getenv("KILL_URL") or "").strip()

    if kill_file:
        p = Path(kill_file)
        if p.exists():
            _warn(f"[KILL_FILE] WARN: file already exists ({p}) — bot will exit on start")
        d = p.parent if p.parent and str(p.parent) != "" else ROOT
        try:
            d.mkdir(parents=True, exist_ok=True)
            probe = d / f"_preflight_kill_probe_{uuid.uuid4().hex}.tmp"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            _pass(f"[KILL_FILE] PASS: directory writable: {d}")
        except Exception as e:
            _fail(f"[KILL_FILE] FAIL: cannot write to directory {d}: {e}")
            ok = False
    else:
        _print("[KILL_FILE] not set")

    if kill_url:
        if offline:
            _print("[KILL_URL] skipped by --offline")
        elif not _HAS_REQUESTS:
            _warn("[KILL_URL] WARN: requests not installed; skipping URL check")
        else:
            try:
                r = requests.head(kill_url, timeout=3)
                if r.status_code >= 400:
                    r = requests.get(kill_url, timeout=5)
                if r.ok:
                    _pass(f"[KILL_URL] PASS: reachable (status {r.status_code})")
                else:
                    _fail(f"[KILL_URL] FAIL: status {r.status_code}")
                    ok = False
            except Exception as e:
                _fail(f"[KILL_URL] FAIL: {e}")
                ok = False
    else:
        _print("[KILL_URL] not set")

    return ok

def check_webhooks(offline: bool = False) -> bool:
    if offline:
        _print("[WEBHOOK] skipped by --offline")
        return True
    names = [
        ("BACKTEST", os.getenv("DISCORD_WEBHOOK_BACKTEST", "").strip()),
        ("PAPER", os.getenv("DISCORD_WEBHOOK_PAPER", "").strip()),
        ("LIVE", os.getenv("DISCORD_WEBHOOK_LIVE", "").strip()),
        ("HEARTBEAT_PAPER", os.getenv("DISCORD_WEBHOOK_HEARTBEAT_PAPER", "").strip()),
        ("HEARTBEAT_LIVE", os.getenv("DISCORD_WEBHOOK_HEARTBEAT_LIVE", "").strip()),
    ]
    if not _HAS_REQUESTS:
        _warn("[WEBHOOK] WARN: requests not installed; skipping webhook checks")
        return True

    ok_all = True
    for label, url in names:
        if not url:
            _print(f"[WEBHOOK] {label}: not set")
            continue
        try:
            post_url = url + "?wait=true"
            content = {"content": f"preflight test: {label} {uuid.uuid4().hex[:8]}"}
            r = requests.post(post_url, json=content, timeout=6)
            if not r.ok:
                _fail(f"[WEBHOOK] FAIL: {label}: POST {r.status_code}")
                ok_all = False
                continue
            msg = r.json()
            msg_id = str(msg.get("id", ""))
            _pass(f"[WEBHOOK] PASS: {label}: post ok (id={msg_id})")
            try:
                del_url = f"{url}/messages/{msg_id}"
                r2 = requests.delete(del_url, timeout=6)
                if r2.status_code in (200, 204):
                    _print(f"[WEBHOOK] {label}: deleted ok")
            except Exception:
                pass
        except Exception as e:
            _fail(f"[WEBHOOK] FAIL: {label}: {e}")
            ok_all = False
    return ok_all

def main():

    parser = argparse.ArgumentParser(description="Preflight checks")
    parser.add_argument("--offline", action="store_true", help="Skip network-dependent checks (Alpaca, webhooks, kill URL)")
    parser.add_argument("--no-smoke", action="store_true", help="Skip smoke backtest")
    args = parser.parse_args()

    total = 0
    passed = 0
    failed = 0
    skipped = 0

    def run_check(label, fn, do=True):
        nonlocal total, passed, failed, skipped
        if not do:
            _print(f"[SKIP] {label}")
            skipped += 1
            return True
        total += 1
        try:
            ok = fn()
        except TypeError:
            # some checks accept offline flag
            try:
                ok = fn(args.offline)  # type: ignore
            except Exception as e:
                _fail(f"[{label}] FAIL: {e}")
                ok = False
        except Exception as e:
            _fail(f"[{label}] FAIL: {e}")
            ok = False
        if ok:
            passed += 1
        else:
            failed += 1
        return ok

    ok = True
    ok &= run_check("LOG_DIR", check_logdir, do=True)
    ok &= run_check("BACKTEST_PARAMS", check_backtest_params, do=True)
    ok &= run_check("ALPACA", check_alpaca, do=(not args.offline))
    ok &= run_check("SMOKE_BACKTEST", lambda: check_smoke_backtest(), do=(not args.no_smoke))
    ok &= run_check("SIDECAR", check_sidecar_and_parquet, do=True)
    ok &= run_check("KILL_CONTROLS", lambda: check_kill_controls(args.offline), do=True)
    ok &= run_check("WEBHOOKS", lambda: check_webhooks(args.offline), do=(not args.offline))

    # Summary line + write json
    _print("")
    summary = {
        "total": total,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "offline": args.offline,
        "no_smoke": args.no_smoke,
        "ts": datetime.now(timezone.utc).isoformat(),
        "log_dir": str(LOG_DIR),
        "data_dir": str(DATA_DIR),
    }
    try:
        (LOG_DIR).mkdir(parents=True, exist_ok=True)
        (LOG_DIR / "preflight_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    except Exception:
        pass

    if failed == 0:
        _print(f"=== PREFLIGHT: PASS=\{passed}/\{total}, SKIP=\{skipped} ===")
        sys.exit(0)
    else:
        _print(f"=== PREFLIGHT: FAILURES=\{failed}, PASS=\{passed}/\{total}, SKIP=\{skipped} ===")
        sys.exit(1)

if __name__ == "__main__":
    main()
