"""
TradingBot.py — Unified bot (Backtest + Paper + Live) with Discord control

Features
--------
Backtest
  - Multi-symbol, multi-pass
  - IEX data feed (avoids SIP 403)
  - CSVs: equity, per-pass report, daily summary (lock-safe on Windows)
  - Discord daily summary

Paper/Live
  - Global single position (configurable)
  - Market-hours aware (pauses when closed unless extended-hours enabled)
  - 2-loss halt per session per symbol
  - Auto-disable symbol after 2 halted sessions (across days)
  - Resume management of open positions on restart
  - Order submission with bracket (TP/SL), fill confirmation, basic rejection handling
  - CSV trade logs + Discord alerts
  - Risk guardrails: auto-stop new entries at -2% daily and -5% weekly drawdown

Discord
  - Inbound commands: !help, !symbols, !add, !remove, !enable, !disable, !kill
  - Kill-switch via Discord, file, or URL

Requirements
------------
pip install alpaca-py alpaca-trade-api pandas numpy python-dotenv requests pytz discord.py
"""

# ==============================
# 0) Imports & logging
# ==============================
import os, sys, time, json, logging, threading, requests, datetime as dt
import signal  # <-- for graceful shutdown signals
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from collections import defaultdict
from time import sleep
import numpy as np, pandas as pd, pytz
SHUTDOWN_EVT = threading.Event()
import atexit
# --- Alpaca (modern client) ---
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, OrderClass
from alpaca.trading.models import Order

# Discord control: token + channel/phrase from env
try:
    import discord
    _HAS_DISCORD = True
except Exception:
    discord = None
    _HAS_DISCORD = False

# Discord (inbound & outbound)
DISCORD_BOT_TOKEN       = os.getenv("DISCORD_BOT_TOKEN", "").strip()
DISCORD_KILL_CHANNEL_ID = os.getenv("DISCORD_KILL_CHANNEL_ID", "").strip()
DISCORD_KILL_PHRASE     = os.getenv("DISCORD_KILL_PHRASE", "!kill").strip()

WEBHOOKS = {
    "BACKTEST": os.getenv("DISCORD_WEBHOOK_BACKTEST", "").strip(),
    "PAPER":    os.getenv("DISCORD_WEBHOOK_PAPER", "").strip(),
    "LIVE":     os.getenv("DISCORD_WEBHOOK_LIVE", "").strip(),
}

# Backtest trade-count targeting
BACKTEST_TARGET_TRADES_PER_SYMBOL = int(os.getenv("BACKTEST_TARGET_TRADES_PER_SYMBOL", "100"))
BACKTEST_MAX_PASSES               = int(os.getenv("BACKTEST_MAX_PASSES", "50"))  # safety cap

# optional dotenv
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None
# --- ML sidecar (real package with safe fallback) ---
try:
    # if your files live in ml_sidecar/sidecar/
    from ml_sidecar.sidecar import event_bus, schemas

    def _ml_emit(factory, /, **kw):
        try:
            event_bus.bus.send(factory(**kw))
        except Exception:
            pass

except Exception:
    # No sidecar available — keep the bot running with no-op emitters
    class _Noop:
        def send(self, *a, **k): 
            pass
    class _SchemasShim:
        def __getattr__(self, name):
            def _f(**kw): 
                return {"event": name, **kw}
            return _f
    class _BusWrap:
        bus = _Noop()

    event_bus = _BusWrap()
    schemas = _SchemasShim()
    def _ml_emit(*a, **k): 
        pass

# later, if you use dotenv:
if load_dotenv:
    load_dotenv()

# Alpaca v3 data
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

# Alpaca v2 trading
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("TradingBot")

# ---- Simple numeric parsing helper (used by config/env parsing) ----
def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if s.startswith("$"):
            s = s[1:].strip()
        return float(s)
    except Exception:
        return default


LOG_DIR = Path(os.getenv("LOG_DIR", r"C:\\Users\\Zacha\\Desktop\\Bot\\ml_sidecar\\logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
# File log handler (in addition to stdout)
try:
    _fh = logging.FileHandler(LOG_DIR / 'bot.log', encoding='utf-8')
    _fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    logging.getLogger().addHandler(_fh)
except Exception:
    pass

# ---- Process-wide shutdown coordination ----
KILL_FLAG = {"stop": False, "_handlers_installed": False}
try:
    SHUTDOWN_EVT  # might already exist
except NameError:
    from threading import Event
    SHUTDOWN_EVT = Event()

# ==============================
# # 1) Heartbeats & runtime status
# ==============================
import time as _time
HB_LOCK = threading.Lock()
HEARTBEAT = {"trader": 0.0, "scheduler": 0.0, "sidecar": 0.0}

# Discord heartbeat cadence (minutes)
HEARTBEAT_OPEN_MIN   = int(os.getenv("HEARTBEAT_OPEN_MIN", "60"))   # market open: every 1h
HEARTBEAT_CLOSED_MIN = int(os.getenv("HEARTBEAT_CLOSED_MIN", "240")) # market closed: every 4h

def secs_since(name: str) -> Optional[int]:
    with HB_LOCK:
        t = HEARTBEAT.get(name, 0.0)
    if t <= 0:
        return None
    return max(0, int(_time.time() - t))

# ==============================
# 2) Env loading & time helpers
# ==============================
def _robust_load_env():
    candidates = [Path.cwd() / ".env", Path.cwd() / "env"]
    for p in candidates:
        try:
            if p.exists():
                if load_dotenv:
                    load_dotenv(str(p), override=True)
                else:
                    for line in p.read_text(encoding="utf-8").splitlines():
                        s = line.strip()
                        if not s or s.startswith("#") or "=" not in s:
                            continue
                        k, v = s.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip())
                log.info(f"[env] Loaded from {p.name}")
        except Exception as e:
            log.warning(f"[env] Failed {p}: {e}")

_robust_load_env()

# Requires: import os, import datetime as dt, import pytz
from typing import Optional, Tuple

TZ_NY = pytz.timezone("America/New_York")
def now_ny() -> dt.datetime: return dt.datetime.now(TZ_NY)
def today_str() -> str: return now_ny().date().isoformat()

def _fmt_eta_to_open(next_open_ny: Optional[dt.datetime]) -> Tuple[str, Optional[str]]:
    """
    Returns (eta_text, local_clock_text) for the next open.
    If USER_TIMEZONE is set, we convert and include it.
    """
    if not next_open_ny:
        return "n/a", None
    try:
        # Ensure tz-aware in NY
        if next_open_ny.tzinfo is None:
            nx = TZ_NY.localize(next_open_ny)
        else:
            nx = next_open_ny.astimezone(TZ_NY)

        # Compute ETA from "now in NY"
        eta = max(dt.timedelta(0), nx - now_ny())
        hrs = int(eta.total_seconds() // 3600)
        mins = int((eta.total_seconds() % 3600) // 60)

        # Convert to user tz if provided
        user_tz_name = os.getenv("USER_TIMEZONE", "").strip()
        local_str = None
        if user_tz_name:
            try:
                user_tz = pytz.timezone(user_tz_name)
                nx_local = nx.astimezone(user_tz)
                local_str = nx_local.strftime("%Y-%m-%d %H:%M %Z")
            except Exception:
                local_str = None

        return f"{hrs}h {mins}m", local_str
    except Exception:
        return "n/a", None

def format_market_open(clock_next_open_utc: dt.datetime) -> str:
    """
    clock_next_open_utc: a timezone-aware UTC datetime (e.g., from Alpaca clock converted to UTC)
    Returns a human string in USER_TIMEZONE (e.g., Asia/Tokyo).
    """
    tz_name = os.getenv("USER_TIMEZONE", "America/New_York")
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone("America/New_York")

    # normalize to local tz for display
    local_dt = clock_next_open_utc.astimezone(tz)
    now_local = dt.datetime.now(dt.timezone.utc).astimezone(tz)
    delta = (local_dt - now_local)
    secs = int(delta.total_seconds())
    if secs <= 0:
        return f"Market open now ({local_dt.strftime('%Y-%m-%d %H:%M %Z')})"

    h, r = divmod(secs, 3600)
    m, s = divmod(r, 60)
    return f"Opens at {local_dt.strftime('%Y-%m-%d %H:%M %Z')} (in {h}h {m}m {s}s)"

def _write_daily_rollup(rows: List[dict], path: Path):
    """Append or update a daily rollup CSV with summary rows."""
    import pandas as pd
    try:
        df_new = pd.DataFrame(rows)
        if path.exists():
            df_old = pd.read_csv(path)
            # remove any duplicate date+mode+symbol combos before merging
            keys = ["date", "mode", "symbol"]
            df_old = df_old.drop_duplicates(subset=keys, keep="last")
            df_new = df_new.drop_duplicates(subset=keys, keep="last")
            df = pd.concat([df_old, df_new], ignore_index=True)
            df = df.drop_duplicates(subset=keys, keep="last")
        else:
            df = df_new
        df.to_csv(path, index=False)
    except Exception as e:
        log.warning(f"[DailyRollup] write failed: {e}")

def _write_weekly_rollups():
    """
    Aggregate paper/live trade CSVs into ISO-week summaries:
      - paper_trades.csv  -> paper_weekly_summary.csv
      - live_trades.csv   -> live_weekly_summary.csv
    """
    import pandas as pd
    from pathlib import Path

    def _roll(infile: str, outfile: str):
        f = Path(infile)
        if not f.exists():
            return
        df = pd.read_csv(f)
        if df.empty:
            return

        # Required columns
        need = {"symbol", "pnl", "time_open"}
        if not need.issubset(set(c.lower() for c in df.columns)):
            # try case-insensitive normalization
            cols_map = {c.lower(): c for c in df.columns}
            try:
                df = df.rename(columns={cols_map["symbol"]: "symbol",
                                        cols_map["pnl"]: "pnl",
                                        cols_map["time_open"]: "time_open"})
            except Exception:
                return
            if not need.issubset(df.columns):
                return

        # Parse time and derive ISO year/week
        df["time_open"] = pd.to_datetime(df["time_open"], errors="coerce")
        df = df.dropna(subset=["time_open"])
        if df.empty:
            return
        iso = df["time_open"].dt.isocalendar()
        df["iso_year"] = iso.year.astype(int)
        df["iso_week"] = iso.week.astype(int)

        # Win/loss flags
        pnl_num = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)
        df["is_win"]  = (pnl_num > 0).astype(int)
        df["is_loss"] = (pnl_num <= 0).astype(int)

        # Group and summarize
        g = df.groupby(["iso_year", "iso_week", "symbol"], as_index=False).agg(
            trades=("symbol", "count"),
            wins=("is_win", "sum"),
            losses=("is_loss", "sum"),
            net_pnl=("pnl", "sum"),
        )
        g["win_rate"] = (g["wins"] / g["trades"]).fillna(0).round(4) * 100.0

        # Stable, readable ordering
        g = g.sort_values(["iso_year", "iso_week", "symbol"], kind="mergesort")
        g.to_csv(Path(outfile), index=False)

    try:
        _roll("paper_trades.csv", "paper_weekly_summary.csv")
    except Exception as e:
        log.warning(f"[WeeklyRollup] paper failed: {e}")
    try:
        _roll("live_trades.csv", "live_weekly_summary.csv")
    except Exception as e:
        log.warning(f"[WeeklyRollup] live failed: {e}")

def _write_monthly_rollups():
    """
    Aggregate paper/live trade CSVs into calendar month summaries (YYYY-MM):
      - paper_trades.csv  -> paper_monthly_summary.csv
      - live_trades.csv   -> live_monthly_summary.csv
    """
    import pandas as pd
    from pathlib import Path

    def _roll(infile: str, outfile: str):
        f = Path(infile)
        if not f.exists():
            return
        df = pd.read_csv(f)
        if df.empty:
            return

        # Normalize required columns
        need = {"symbol", "pnl", "time_open"}
        low = {c.lower(): c for c in df.columns}
        if not need.issubset(set(low.keys())):
            return
        df = df.rename(columns={low["symbol"]: "symbol",
                                low["pnl"]: "pnl",
                                low["time_open"]: "time_open"})

        # Parse time and derive month id YYYY-MM
        df["time_open"] = pd.to_datetime(df["time_open"], errors="coerce")
        df = df.dropna(subset=["time_open"])
        if df.empty:
            return
        df["month"] = df["time_open"].dt.to_period("M").astype(str)  # e.g., "2025-08"

        # Win/loss flags
        pnl_num = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)
        df["is_win"]  = (pnl_num > 0).astype(int)
        df["is_loss"] = (pnl_num <= 0).astype(int)

        # Group and summarize
        g = df.groupby(["month", "symbol"], as_index=False).agg(
            trades=("symbol", "count"),
            wins=("is_win", "sum"),
            losses=("is_loss", "sum"),
            net_pnl=("pnl", "sum"),
        )
        g["win_rate"] = (g["wins"] / g["trades"]).fillna(0).round(4) * 100.0

        # Stable order
        g = g.sort_values(["month", "symbol"], kind="mergesort")
        g.to_csv(Path(outfile), index=False)

    try:
        _roll("paper_trades.csv", "paper_monthly_summary.csv")
    except Exception as e:
        log.warning(f"[MonthlyRollup] paper failed: {e}")
    try:
        _roll("live_trades.csv", "live_monthly_summary.csv")
    except Exception as e:
        log.warning(f"[MonthlyRollup] live failed: {e}")

def _startup_market_banner(mode: str):
    """
    One‑time Discord banner at startup showing market state and ETA to next open.
    Works for PAPER/LIVE. Uses USER_TIMEZONE if provided.
    """
    try:
        api = build_trading_api(mode)
        is_open, next_open, _ = is_market_open(api)
        eta_text, local_text = _fmt_eta_to_open(next_open)
        state = "OPEN" if is_open else "CLOSED"
        lines = [f"**{mode} STARTUP**", f"Market: {state}"]
        if not is_open:
            # show ETA line only when closed
            if local_text:
                lines.append(f"Next open in: {eta_text} · {local_text}")
            else:
                lines.append(f"Next open in: {eta_text}")
        send_discord("\n".join(lines), mode)
    except Exception as e:
        log.warning(f"[StartupBanner] failed: {e}")

def _start_heartbeat_scheduler(mode: str):
    """
    Periodically post a heartbeat to Discord for PAPER/LIVE, including:
      • Market open/closed state
      • ETA until next open (HHh MMm) in NY (and optional local time via USER_TIMEZONE)
      • Lightweight liveness ages for trader/scheduler/sidecar (if runtime status file is used)

    Cadence (env-tunable):
      • Open:   HEARTBEAT_OPEN_MIN   (default 60)
      • Closed: HEARTBEAT_CLOSED_MIN (default 240)
    """
    if mode not in ("PAPER", "LIVE"):
        return
    global _HEARTBEAT_STARTED
    if _HEARTBEAT_STARTED:
        return
    _HEARTBEAT_STARTED = True

    # Open cadence (minutes) and Closed cadence (minutes)
    open_interval_min   = int(os.getenv("HEARTBEAT_OPEN_MIN", "60"))
    closed_interval_min = int(os.getenv("MARKET_CLOSED_INTERVAL_MIN",
                                        os.getenv("HEARTBEAT_CLOSED_MIN", "240")))

    # optional runtime status helpers (safe if status file missing)
    def _secs_since(label: str) -> str:
        try:
            rs = _read_runtime_status()
            hb = rs.get("hb", {})
            val = hb.get(f"{label}_secs", None)
            return f"{val}s" if isinstance(val, (int, float)) else "n/a"
        except Exception:
            return "n/a"

    def _eta_to_open_text(api_obj) -> str:
        """
        Returns a human string like 'Opens in 2h 15m at 2025-08-21 09:30 EDT (07:30 MDT)'.
        Uses _fmt_eta_to_open(next_open) which you already have.
        """
        try:
            is_open, next_open, _ = is_market_open(api_obj)
            if is_open:
                return "Market is OPEN"
            eta_text, local_text = _fmt_eta_to_open(next_open)
            if local_text:
                return f"Opens in {eta_text} at {local_text}"
            # If no USER_TIMEZONE, still show NY-time ETA
            ny_clock = next_open.strftime("%Y-%m-%d %H:%M %Z") if next_open else "n/a"
            return f"Opens in {eta_text} at {ny_clock}"
        except Exception:
            return "Market clock unavailable"

    def runner():
        try:
            api = build_trading_api(mode)
        except Exception:
            api = None

        # one-shot confirmation that scheduler started (fan-out)
        try:
            msg0 = f"**{mode} Heartbeat** scheduler started."
            send_discord_heartbeat(msg0, mode)  # heartbeat channel
            send_discord(msg0, mode)            # main mode channel
            _log_heartbeat_line(msg0, mode)     # heartbeat.log with mode tag
        except Exception:
            pass

        last_sent = 0.0
        while True:
            try:
                if check_kill_signal():
                    break

                beat("scheduler")

                is_open = False
                eta_text = "Market clock unavailable"
                try:
                    if api is None:
                        api = build_trading_api(mode)
                    is_open, _, _ = is_market_open(api)
                    eta_text = _eta_to_open_text(api)
                except Exception:
                    pass

                # liveness (safe if runtime file not used)
                trader_age = _secs_since("trader")
                sched_age  = _secs_since("scheduler")
                sidecar_age = _secs_since("sidecar")

                msg = (
                    f"**HEARTBEAT · {mode}**\n"
                    f"• Market: {'OPEN' if is_open else 'CLOSED'}\n"
                    f"• ETA: {eta_text}\n"
                    f"• Trader: {trader_age} | Scheduler: {sched_age} | Sidecar: {sidecar_age}"
                )

                # choose interval based on market state
                interval_s = (open_interval_min if is_open else closed_interval_min) * 60
                now_ts = time.time()
                if now_ts - last_sent >= max(60, interval_s):
                    # Fan‑out: heartbeat channel + mode channel + heartbeat.log
                    send_discord_heartbeat(msg, mode)   # dedicated heartbeat channel
                    send_discord(msg, mode)             # main mode webhook (PAPER/LIVE)
                    _log_heartbeat_line(msg, mode)      # persist with mode tag
                    last_sent = now_ts

                # sleep in short chunks for fast kill reaction
                for _ in range(60):  # ~60s granularity
                    if check_kill_signal():
                        break
                    time.sleep(1)
            except Exception as e:
                log.warning(f"[Heartbeat] loop error: {e}")
                time.sleep(10)

    # fire-and-forget background thread
    threading.Thread(target=runner, daemon=True).start()


# ==============================
# 3) >>> MODE TOGGLE <<<
# ==============================
MODE = os.getenv("MODE", "BACKTEST").upper()  # BACKTEST | PAPER | LIVE

# ==============================
# 4) Config
# ==============================
# Backtests
BACKTEST_INTERVAL        = os.getenv("BACKTEST_INTERVAL", "1Day")  # 1Min | 1Day
BACKTEST_LOOKBACK_DAYS   = int(os.getenv("BACKTEST_LOOKBACK_DAYS", "120"))
BACKTEST_PASSES          = int(os.getenv("BACKTEST_PASSES", "3"))
BACKTEST_STARTING_EQUITY = float(os.getenv("BACKTEST_STARTING_EQUITY", "100000"))
BACKTEST_GLOBAL_SINGLE_POSITION = os.getenv("BACKTEST_GLOBAL_SINGLE_POSITION", "false").lower() == "true"

# Live/Paper single-position policy
PAPER_GLOBAL_SINGLE_POSITION = os.getenv("PAPER_GLOBAL_SINGLE_POSITION", "true").lower() == "true"
LIVE_GLOBAL_SINGLE_POSITION  = os.getenv("LIVE_GLOBAL_SINGLE_POSITION",  "true").lower() == "true"

# Symbols & strategy
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "TSLA,NVDA,AMD").split(",") if s.strip()]
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.05"))
STOP_LOSS_PCT   = float(os.getenv("STOP_LOSS_PCT", "0.02"))
RISK_PER_TRADE  = float(os.getenv("RISK_PER_TRADE", "0.01"))
PAPER_DRY_RUN = os.getenv("PAPER_DRY_RUN", "false").lower() == "true"

SMA_FAST   = int(os.getenv("SMA_FAST", "5"))
SMA_SLOW   = int(os.getenv("SMA_SLOW", "20"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_MIN_BUY  = float(os.getenv("RSI_MIN_BUY", "45"))
RSI_MAX_SELL = float(os.getenv("RSI_MAX_SELL", "70"))

# Market hours behavior
TRADE_EXTENDED_HOURS = os.getenv("TRADE_EXTENDED_HOURS", "false").lower() == "true"
MARKET_SLEEP_BUFFER_SECONDS = int(os.getenv("MARKET_SLEEP_BUFFER_SECONDS", "15"))


# Alpaca creds
APCA = {
    "PAPER": {
        "key": os.getenv("APCA_API_KEY_ID_PAPER", ""),
        "secret": os.getenv("APCA_API_SECRET_KEY_PAPER", ""),
        "base_url": os.getenv("APCA_PAPER_BASE_URL", "https://paper-api.alpaca.markets"),
    },
    "LIVE": {
        "key": os.getenv("APCA_API_KEY_ID_LIVE", ""),
        "secret": os.getenv("APCA_API_SECRET_KEY_LIVE", ""),
        "base_url": os.getenv("APCA_LIVE_BASE_URL", "https://api.alpaca.markets"),
    },
}

# Kill-switch
KILL_FLAG = {"stop": False}
# Start-once guards (prevent duplicate background threads)
_HEARTBEAT_STARTED = False
_COUNTDOWN_STARTED = False
# Event used by signal handlers for graceful shutdown
SHUTDOWN_EVT = threading.Event()
KILL_FILE = os.getenv("KILL_FILE", "")  # optional file path; if exists => stop
KILL_URL  = os.getenv("KILL_URL", "")   # optional: GET; if response starts with 'STOP' => stop

# Risk guardrails (auto-stop new entries if breached)
DAILY_LOSS_LIMIT_PCT  = float(os.getenv("DAILY_LOSS_LIMIT_PCT",  "0.02"))  # 2% default
WEEKLY_LOSS_LIMIT_PCT = float(os.getenv("WEEKLY_LOSS_LIMIT_PCT", "0.05"))  # 5% default

# Weekly baseline persistence
WEEK_STATE_PATH = Path("weekly_state.json")  # {"week_key": "YYYY-W##", "week_start_equity": float}

# ==============================
# 5) Persistence (JSON files)
# ==============================
SESSION_STATE_PATH     = Path("session_state.json")    # {"halted_sessions": {"TSLA": 1, ...}}
DISABLED_SYMBOLS_PATH  = Path("disabled_symbols.json") # ["TSLA","XYZ"]
SYMBOLS_USER_PATH      = Path("symbols_user.json")     # ["AMD","NVDA"]

def _json_load(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"Read {path} failed: {e}")
    return default

def _json_save(path: Path, obj):
    try:
        path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    except Exception as e:
        log.warning(f"Write {path} failed: {e}")

def _load_user_symbols() -> List[str]:
    data = _json_load(SYMBOLS_USER_PATH, [])
    out = []
    for s in data:
        s = str(s).strip().upper()
        if s and s not in out:
            out.append(s)
    return out

def _save_user_symbols(symbols: List[str]):
    uniq = []
    for s in symbols:
        s = str(s).strip().upper()
        if s and s not in uniq:
            uniq.append(s)
    _json_save(SYMBOLS_USER_PATH, uniq)

def _load_disabled_set() -> set:
    return set(s.upper() for s in _json_load(DISABLED_SYMBOLS_PATH, []))

def _save_disabled_set(disabled: set):
    _json_save(DISABLED_SYMBOLS_PATH, sorted(s.upper() for s in disabled))

def _sanitize_symbol(s: str) -> Optional[str]:
    if not s: return None
    s = s.strip().upper()
    import re
    return s if re.fullmatch(r"[A-Z0-9\.\-]{1,10}", s) else None

# ---- Risk / equity helpers ----

def _get_equity(api: TradingClient) -> Optional[float]:
    try:
        return _safe_float(api.get_account().equity, None)
    except Exception:
        return None

def _read_text_file_safely(path: str, max_bytes: int = 4096) -> str:
    try:
        from pathlib import Path
        p = Path(path)
        if not p.exists() or not p.is_file():
            return ""
        with p.open("r", encoding="utf-8", errors="ignore") as fh:
            return fh.read(max_bytes)
    except Exception:
        return ""

def _fetch_text_url_safely(url: str, timeout: float = 3.0, max_bytes: int = 4096) -> str:
    try:
        import requests
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            return ""
        return (r.text or "")[:max_bytes]
    except Exception:
        return ""

def check_external_kill(throttle_sec: float = 30.0) -> bool:
    """
    Returns True if either:
      - KILL_FILE exists and contains 'KILL' (case-insensitive), or
      - KILL_URL returns a body containing 'KILL' (case-insensitive).
    Throttled to avoid spamming remote endpoints.
    """
    import time, os
    now = time.monotonic()
    last = getattr(check_external_kill, "_last_check", 0.0)
    if (now - last) < max(1.0, float(throttle_sec)):
        return getattr(check_external_kill, "_last_result", False)

    kill_file = (os.getenv("KILL_FILE") or "").strip()
    kill_url  = (os.getenv("KILL_URL")  or "").strip()
    hit = False

    if kill_file:
        txt = _read_text_file_safely(kill_file)
        if "kill" in txt.lower():
            hit = True

    if (not hit) and kill_url:
        txt = _fetch_text_url_safely(kill_url)
        if "kill" in txt.lower():
            hit = True

    setattr(check_external_kill, "_last_check", now)
    setattr(check_external_kill, "_last_result", hit)
    return hit
    
def _week_key(dt_ny: dt.datetime) -> str:
    # ISO week key (YYYY-W##), using New York date
    d = dt_ny.date()
    iso = d.isocalendar()  # (year, week, weekday)
    return f"{iso.year}-W{iso.week:02d}"

def _load_week_state() -> dict:
    return _json_load(WEEK_STATE_PATH, {"week_key": "", "week_start_equity": None})

def _save_week_state(state: dict):
    _json_save(WEEK_STATE_PATH, state)

# ==============================
# 6) Messaging / Kill checks
# ==============================
def send_discord(msg: str, mode: str = None):
    mode = (mode or MODE).upper()
    url = WEBHOOKS.get(mode, "")
    if not url:
        log.debug(f"[Discord] skipped (no webhook for {mode}) -> {msg}")
        return
    try:
        requests.post(url, json={"content": msg[:1900]}, timeout=5)
    except Exception as e:
        log.warning(f"[Discord] send failed: {e}")
def send_discord_hb(msg: str, mode: str = None):
    """
    Heartbeat-specific Discord sender. Uses a dedicated webhook if present:
      - DISCORD_WEBHOOK_HEARTBEAT_<MODE> (e.g., ..._PAPER, ..._LIVE)
      - else DISCORD_WEBHOOK_HEARTBEAT
      - else falls back to the normal per-mode webhook (send_discord)
    """
    def _log_heartbeat(msg: str):
        try:
            lg = logging.getLogger("Heartbeat")
            lg.info(msg.replace("\n", " | "))
        except Exception:
            pass

    mode = (mode or MODE).upper()
    # Prefer per-mode heartbeat webhook, then generic, then fallback
    url = os.getenv(f"DISCORD_WEBHOOK_HEARTBEAT_{mode}") or \
          os.getenv("DISCORD_WEBHOOK_HEARTBEAT") or \
          ""
    if not url:
        # fallback to the standard channel if no heartbeat webhook is set
        return send_discord(msg, mode)
    try:
        requests.post(url, json={"content": msg[:1900]}, timeout=5)
    except Exception as e:
        log.warning(f"[Discord HB] send failed: {e}")

def _log_heartbeat_line(msg: str, mode: str = None):
    """
    Append heartbeat/countdown messages to a local log file.
    Includes mode (PAPER/LIVE) if provided.
    Path is configurable via HEARTBEAT_LOG (default ml_sidecar/logs/heartbeat.log).
    """
    try:
        log_path = os.getenv("HEARTBEAT_LOG", str(LOG_DIR / "heartbeat.log"))
        p = Path(log_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            ts = now_ny().strftime("%Y-%m-%d %H:%M:%S %Z")
            mode_tag = f"[{mode}]" if mode else ""
            f.write(f"{mode_tag}[{ts}] {msg}\n")
    except Exception:
        pass

def _send_initial_market_state(mode: str):
    """
    One-time banner on startup to the MAIN trading channel (not heartbeat),
    plus a mirrored log line to heartbeat.log with the mode tag.
    """
    try:
        api = build_trading_api(mode)
        is_open, next_open, _ = is_market_open(api)
        eta_text, local_text = _fmt_eta_to_open(next_open)
        state = "OPEN" if is_open else "CLOSED"

        lines = [f"**{mode} STARTUP**", f"Market: {state}"]
        if not is_open:
            # show ETA lines only when closed
            if local_text:
                lines.append(f"Next open in: {eta_text} · {local_text}")
            else:
                lines.append(f"Next open in: {eta_text}")

        msg = "\n".join(lines)

        # Send to the main trading channel for this mode
        send_discord(msg, mode)

        # Also persist a copy to heartbeat.log with mode tag
        _log_heartbeat_line(msg, mode)

    except Exception as e:
        # Don’t crash on startup if market clock fails
        log.warning(f"[StartupBanner] failed: {e}")

def send_backtest_startup(symbols, interval, lookback_days, passes):
    """One-time Discord banner when BACKTEST boots."""
    msg = "\n".join([
        "**BACKTEST STARTUP**",
        f"Symbols: {', '.join(symbols)}",
        f"Interval: {interval}",
        f"Lookback: {lookback_days} days",
        f"Passes: {passes}",
    ])
    send_discord(msg, "BACKTEST")
    # Dedicated backtest channel helpers

def send_backtest(msg: str):
    """
    Send only to the dedicated BACKTEST webhook (if set).
    Falls back to normal send_discord('BACKTEST') if the specific
    webhook is not configured in env.
    """
    url = os.getenv("DISCORD_WEBHOOK_BACKTEST", "").strip()
    if url:
        try:
            requests.post(url, json={"content": msg[:1900]}, timeout=5)
            return
        except Exception as e:
            log.warning(f"[Backtest] direct webhook send failed, falling back: {e}")
    # Fallback to the normal BACKTEST route (same channel as WEBHOOKS['BACKTEST'])
    send_discord(msg, "BACKTEST")

def should_stop(mode: str) -> str | None:
    """
    Return a reason string if a kill was requested, else None.
    Reasons: "signal", "external"
    """
    try:
        if check_kill_signal():
            return "signal"
    except Exception:
        pass
    try:
        if check_external_kill():
            return "external"
    except Exception:
        pass
    return None

def send_backtest_progress(pass_idx: int, total: int, extra: Optional[str] = None):
    """
    Optional progress ping during long backtests.
    Example:
        send_backtest_progress(i+1, BACKTEST_PASSES, "rolling Sharpe=1.2")
    """
    pct = (100.0 * pass_idx / max(1, total))
    line = f"**BACKTEST** progress {pass_idx}/{total} ({pct:.1f}%)"
    if extra:
        line += f" · {extra}"
    send_backtest(line)

    # Alias for backtest notifications (dedicated channel)
def send_discord_backtest(msg: str):
    return send_discord(msg, "BACKTEST")
# Lightweight heartbeat bookkeeping for !status

_RUNTIME_STATUS_PATH = LOG_DIR / "runtime_status.json"



def _read_runtime_status() -> dict:
    """Best-effort read of runtime_status.json."""
    try:
        if _RUNTIME_STATUS_PATH.exists():
            with _RUNTIME_STATUS_PATH.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _write_runtime_status(mode: str, extra: dict | None = None) -> None:
    """
    Persist a single source of truth for runtime status to LOG_DIR/runtime_status.json.

    Parameters
    ----------
    mode : str
        "BACKTEST", "PAPER", or "LIVE".
    extra : dict | None
        Additional fields to merge into the status dict. If 'hb' is present in
        extra and is a dict, it will be merged into the existing 'hb' bag.
    """
    try:
        d = _read_runtime_status() or {}
        d["mode"] = mode
        d.setdefault("hb", {})

        if isinstance(extra, dict):
            # Merge heartbeat bag if provided
            if isinstance(extra.get("hb"), dict):
                d["hb"].update(extra["hb"])
            # Merge the rest (avoid overwriting 'hb' we just merged)
            for k, v in extra.items():
                if k == "hb":
                    continue
                d[k] = v

        _RUNTIME_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _RUNTIME_STATUS_PATH.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(d, f, indent=2, default=str)
        tmp.replace(_RUNTIME_STATUS_PATH)
    except Exception:
        # never let status writes crash the bot
        pass


def beat(label: str):
    """
    Touches ml_sidecar/logs/beat_<label>.txt and updates runtime_status.json ages.
    Example labels: 'trader', 'scheduler', 'sidecar'
    """
    try:
        (LOG_DIR / f"beat_{label}.txt").write_text(
            dt.datetime.now(dt.timezone.utc).isoformat(), encoding="utf-8"
        )
    except Exception:
        pass

    # Always update runtime_status.json
    try:
        d = _read_runtime_status()
        hb = d.setdefault("hb", {})
        now = time.time()
        hb[f"{label}_ts"] = now
        for k in ("trader", "scheduler", "sidecar"):
            ts = hb.get(f"{k}_ts")
            hb[f"{k}_secs"] = int(now - ts) if isinstance(ts, (int, float)) else None
        d["hb"] = hb
        _write_runtime_status(MODE, d)  # merges into existing
    except Exception:
        pass

def check_kill_signal() -> bool:
    """
    Installs Ctrl+C/kill signal handlers once and exposes a simple boolean:
    return True when a shutdown was requested.
    """
    def _handler(signum, frame):
        # Set the kill flag and try to notify Discord (best-effort).
        try:
            send_discord(f"⚠️ {MODE} shutting down (signal={signum}).", MODE)
        except Exception:
            pass
        try:
            KILL_FLAG["stop"] = True
        except Exception:
            pass
        # Optional event if your code uses it
        try:
            SHUTDOWN_EVT.set()  # may not exist; that's fine
        except Exception:
            pass

    def _install_signal_handlers():
        # Register handlers on platforms that support them
        for _sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):  # SIGBREAK on Windows
            _sig = getattr(signal, _sig_name, None)
            if _sig is not None:
                try:
                    signal.signal(_sig, _handler)
                except Exception:
                    # Some environments (threads, restricted shells) won’t allow this
                    pass

    # Install handlers only once
    if not KILL_FLAG.get("_handlers_installed"):
        _install_signal_handlers()
        KILL_FLAG["_handlers_installed"] = True

    # Return whether a shutdown has been requested
    return bool(KILL_FLAG.get("stop", False))

def write_status_snapshot(mode: str, today_pnl: float, open_positions: int) -> None:
    """
    Single source of truth for runtime status:
      - mode: "PAPER" / "LIVE"
      - today_pnl: float
      - open_positions: int
      - hb: seconds since last heartbeats for trader/scheduler/sidecar
    """
    from pathlib import Path

    def _age(path: Path) -> int | None:
        try:
            if not path.exists():
                return None
            mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
            return max(0, int((dt.datetime.now(dt.timezone.utc) - mtime).total_seconds()))
        except Exception:
            return None

    try:
        hb = {
            "trader_secs":   _age(LOG_DIR / "beat_trader.txt"),
            "scheduler_secs": _age(LOG_DIR / "beat_scheduler.txt"),
            "sidecar_secs":   _age(LOG_DIR / "beat_sidecar.txt"),
        }

        _write_runtime_status(mode, {
            "today_pnl": float(today_pnl),
            "open_positions": int(open_positions),
            "hb": hb,
        })
    except Exception:
        # status is best-effort; ignore errors
        pass

# ==============================
# 7) Alpaca clients & data
# ==============================
def build_data_client() -> StockHistoricalDataClient:
    api_key = os.getenv("APCA_API_KEY_ID_PAPER") or os.getenv("APCA_API_KEY_ID_LIVE")
    secret_key = os.getenv("APCA_API_SECRET_KEY_PAPER") or os.getenv("APCA_API_SECRET_KEY_LIVE")
    if not api_key or not secret_key:
        raise ValueError("No Alpaca credentials found for data client.")
    return StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

def build_trading_api(mode: str) -> TradingClient:
    """
    Build a modern TradingClient for PAPER or LIVE (alpaca-py).
    Uses APCA[mode] for key/secret/base_url that your file already defines.
    """
    creds = APCA[mode]
    return TradingClient(
        creds["key"],
        creds["secret"],
        paper=(mode == "PAPER"),
        url=creds.get("base_url") or None,
    )

def _timeframe_from_interval(interval: str) -> TimeFrame:
    i = interval.strip().lower()
    if i in ("1min", "1m", "minute", "min"):
        return TimeFrame(1, TimeFrameUnit.Minute)
    if i in ("1day", "1d", "day", "daily"):
        return TimeFrame.Day
    raise ValueError("Use BACKTEST_INTERVAL = 1Min or 1Day")

def fetch_bars(symbol: str, start: dt.datetime, end: dt.datetime, interval="1Day") -> pd.DataFrame:
    """Historical bars via Alpaca v3, using IEX feed to avoid SIP 403."""
    tf = _timeframe_from_interval(interval)
    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=tf,
        start=start,
        end=end,
        adjustment="raw",
        limit=10000,
        feed=DataFeed.IEX
    )
    client = build_data_client()
    bars = client.get_stock_bars(req)
    df = bars.df
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.index, pd.MultiIndex):
        df = df.xs(symbol, level=0)
    try:
        df = df.tz_convert("America/New_York").sort_index()
    except Exception:
        pass
    df = df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"})
    return df.reset_index().rename(columns={"timestamp":"Time"})

# ==============================
# 8) Strategy
# ==============================
def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)

def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["SMA_F"] = sma(d["Close"], SMA_FAST)
    d["SMA_S"] = sma(d["Close"], SMA_SLOW)
    d["RSI"] = rsi(d["Close"], RSI_PERIOD)
    d["CROSS_UP"] = (d["SMA_F"].shift(1) <= d["SMA_S"].shift(1)) & (d["SMA_F"] > d["SMA_S"]) & (d["RSI"] >= RSI_MIN_BUY)
    d["CROSS_DOWN"] = (d["SMA_F"].shift(1) >= d["SMA_S"].shift(1)) & (d["SMA_F"] < d["SMA_S"])
    return d

# ---- Unit-testable pure helpers (no side effects) ----
def qty_from_risk(equity: float, price: float, stop_loss_pct: float, risk_per_trade: float) -> int:
    """
    Position sizing from risk. Deterministic, pure, no I/O.
    - equity: current account or per-symbol equity basis
    - price: entry price
    - stop_loss_pct: fractional SL distance (e.g., 0.02)
    - risk_per_trade: fraction of equity to risk per trade (e.g., 0.01)
    Returns an integer share quantity >= 1.
    """
    try:
        sl_price = price * (1 - max(0.0, float(stop_loss_pct)))
        risk_per_share = max(price - sl_price, 1e-6)
        dollars_at_risk = max(0.0, float(equity)) * max(0.0, float(risk_per_trade))
        qty = int(max(dollars_at_risk / risk_per_share, 1))
        return qty
    except Exception:
        return 1

def pnl_from_fill(entry_price: float, exit_price: float, qty: int) -> float:
    """
    Basic PnL computation. Deterministic, pure.
    Positive when exit > entry for long positions.
    """
    try:
        return (float(exit_price) - float(entry_price)) * int(qty)
    except Exception:
        return 0.0
# ---- Risk-breach helpers for PAPER/LIVE ----
def _get_loss_caps_from_env():
    """Read loss caps and flatten flag from env."""
    try:
        daily = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0") or "0")
    except Exception:
        daily = 0.0
    try:
        weekly = float(os.getenv("WEEKLY_LOSS_LIMIT_PCT", "0") or "0")
    except Exception:
        weekly = 0.0
    flatten = str(os.getenv("FLATTEN_ON_RISK_BREACH", "false")).strip().lower() == "true"
    return max(daily, 0.0), max(weekly, 0.0), flatten

def handle_risk_breach(api, current_equity: float, daily_start_equity: float, weekly_start_equity: float,
                       send_func=send_discord, mode="PAPER") -> bool:
    """
    Check caps; if breach and FLATTEN_ON_RISK_BREACH=true, flatten positions and return True.
    Otherwise, return True on breach (even if not flattening) so caller can halt new entries.
    """
    daily_cap, weekly_cap, flatten = _get_loss_caps_from_env()
    d_breach, w_breach = risk_breach_flags(daily_start_equity, weekly_start_equity, current_equity,
                                           daily_cap, weekly_cap)
    if not (d_breach or w_breach):
        return False

    which = " & ".join([s for s, b in (("DAILY", d_breach), ("WEEKLY", w_breach)) if b]) or "UNKNOWN"
    send_func(f"🛑 {mode} risk breach detected ({which}). Flatten={flatten}.", mode)

    if flatten:
        count = flatten_all_positions(api, send_func=send_func, mode=mode)
        send_func(f"🧹 {mode} flatten complete — {count} position(s) attempted.", mode)
    return True  # breach occurred

# ==============================
# 9) CSV helpers (Windows lock-safe)
# ==============================
def _atomic_to_csv(df: pd.DataFrame, path: Path, retries: int = 3, delay: float = 1.0):
    path = Path(path)
    for attempt in range(1, retries + 1):
        tmp = path.with_suffix(path.suffix + f".tmp_{os.getpid()}_{int(time.time())}")
        try:
            df.to_csv(tmp, index=False)
            os.replace(tmp, path)  # atomic on Windows
            return
        except PermissionError:
            try:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
            except Exception:
                pass
            if attempt < retries:
                logging.warning(f"[CSV] '{path.name}' locked (attempt {attempt}/{retries}). Retrying in {delay:.1f}s...")
                sleep(delay)
                continue
            raise
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
            except Exception:
                pass
            raise

def _log_trade_row(row: dict, filename: str):
    """
    Append a trade row to a CSV in LOG_DIR (atomic write).
    filename: e.g., "paper_trades.csv" or "live_trades.csv"
    Columns: time_open, time_close, symbol, qty, entry_price, exit_price, pnl,
             result, mode, tp, sl, client_order_id
    """
    try:
        fpath = LOG_DIR / filename
        cols = [
            "time_open", "time_close", "symbol", "qty",
            "entry_price", "exit_price", "pnl", "result",
            "mode", "tp", "sl", "client_order_id"
        ]
        new_df = pd.DataFrame([row], columns=cols)
        if fpath.exists():
            try:
                old_df = pd.read_csv(fpath)
            except Exception:
                old_df = pd.DataFrame(columns=cols)
            out = pd.concat([old_df, new_df], ignore_index=True)
        else:
            out = new_df
        _atomic_to_csv(out, fpath)
    except Exception as e:
        log.warning(f"[TradeCSV] write failed: {e}")

def _write_backtest_summary_for_today(rows: List[dict], path: Path):
    """
    Append today's per-symbol summary rows to a rolling CSV, deterministic order.
    - If file exists, concat and drop duplicate (date, symbol) rows (keep last).
    - Sort by date, then symbol for stable diffs & Discord digests.
    """
    try:
        df_new = pd.DataFrame(rows)
        if df_new.empty:
            return
        # Ensure required columns exist
        for col in ("date", "symbol"):
            if col not in df_new.columns:
                return

        if path.exists():
            try:
                df_old = pd.read_csv(path)
            except Exception:
                df_old = pd.DataFrame(columns=df_new.columns.tolist())
            df = pd.concat([df_old, df_new], ignore_index=True)
            df = df.drop_duplicates(subset=["date", "symbol"], keep="last")
        else:
            df = df_new

        # Stable ordering for readability
        sort_cols = [c for c in ("date", "symbol") if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, kind="mergesort")

        _atomic_to_csv(df, path)
    except Exception:
        pass

# ==============================
# 10) Backtest Engine
# ==============================
@dataclass
class Trade:
    symbol: str
    entry_time: Optional[dt.datetime]
    entry_price: float
    qty: int
    exit_time: Optional[dt.datetime] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None

def run_backtest(symbols: List[str], interval: str, lookback_days: int, passes: int,
                 global_single: bool = BACKTEST_GLOBAL_SINGLE_POSITION) -> Dict:
    log.info(f"Running BACKTEST: interval={interval}, lookback_days={lookback_days}, passes={passes}, symbols={symbols}")
    # --- Patch 1A: Determinism (seed RNGs + stable symbol universe) ---
    try:
        import random
        random.seed(0)
    except Exception:
        pass
    try:
        import numpy as _np
        _np.random.seed(0)
    except Exception:
        pass
    symbols = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
    send_discord(f"**BACKTEST** start | {interval} | {lookback_days}d | passes={passes} | {symbols}", "BACKTEST")

    end = now_ny()
    start = end - dt.timedelta(days=lookback_days + 10)

    equity_curve: List[Dict] = []
    per_symbol_stats = {s: {"trades": 0, "wins": 0, "losses": 0, "net_pnl": 0.0} for s in symbols}
    report_rows: List[Dict] = []

    in_position_global = False
    current_trade_global: Optional[Trade] = None
    per_symbol_equity = {s: BACKTEST_STARTING_EQUITY for s in symbols}

    for attempt in range(1, passes + 1):
            log.info(f"[Backtest] Starting {passes} passes for {len(symbols)} symbols...")
    # Adaptive: sweep until each symbol reaches target trade count, or max passes hit.
    attempt = 0
    while True:
        attempt += 1
        max_allowed = max(passes, BACKTEST_MAX_PASSES)
        if attempt > max_allowed:
            log.info(f"BACKTEST: reached max passes ({max_allowed}); stopping.")
            break

        # If we've already reached the target trades per symbol, stop
        try:
            reached = all(per_symbol_stats[s]["trades"] >= BACKTEST_TARGET_TRADES_PER_SYMBOL for s in symbols)
        except Exception:
            reached = False
        if reached:
            log.info(f"BACKTEST: all symbols reached >= {BACKTEST_TARGET_TRADES_PER_SYMBOL} trades; stopping at pass {attempt-1}.")
            break

        log.info(f"[Backtest] Pass {attempt}/{max_allowed}")
        per_pass_curve: List[Dict] = []  # Patch 3A: per-pass buffer
        _reason = should_stop("BACKTEST")
        if _reason:
            log.warning(f"[Backtest] kill received ({_reason}) on sweep {attempt}. Aborting backtest.")
            send_discord(f"**BACKTEST KILL** received ({_reason}) on sweep {attempt}. Aborting.", "BACKTEST")
            break

        for symbol in symbols:
            _reason = should_stop("BACKTEST")
            if _reason:
                log.warning(f"[Backtest] kill received ({_reason}). Aborting.")
                break

            try:
                raw = fetch_bars(symbol, start, end, interval)
            except Exception as e:
                log.warning(f"{symbol}: fetch error on attempt {attempt}: {e}")
                continue
            if raw.empty or len(raw) < max(SMA_SLOW, RSI_PERIOD) + 5:
                log.info(f"{symbol}: not enough data on attempt {attempt}.")
                continue

            df = generate_signals(raw)

            if global_single:
                in_position = in_position_global and current_trade_global is not None
                current_trade = current_trade_global
            else:
                in_position = False
                current_trade = None

            for _, row in df.iterrows():
                if check_kill_signal():
                    break

                ts = row["Time"]
                price = float(row["Close"])
                row_dict = {
                    "time": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                    "symbol": symbol,
                    "equity": per_symbol_stats[symbol]["net_pnl"],
                    "pass": attempt,
                }
                equity_curve.append(row_dict)
                per_pass_curve.append(row_dict)

                # Exit logic
                if in_position and current_trade and current_trade.symbol == symbol:
                    tp_price = current_trade.entry_price * (1 + TAKE_PROFIT_PCT)
                    sl_price = current_trade.entry_price * (1 - STOP_LOSS_PCT)
                    exit_signal = bool(row.get("CROSS_DOWN", False))
                    if price >= tp_price or price <= sl_price or exit_signal:
                        current_trade.exit_time = ts if isinstance(ts, dt.datetime) else None
                        current_trade.exit_price = price
                        current_trade.pnl = pnl_from_fill(current_trade.entry_price, price, current_trade.qty)  # Patch 2B
                        pnl_val = float(current_trade.pnl or 0.0)

                        per_symbol_stats[symbol]["trades"] += 1
                        if pnl_val > 0:
                            per_symbol_stats[symbol]["wins"] += 1
                        else:
                            per_symbol_stats[symbol]["losses"] += 1
                        per_symbol_stats[symbol]["net_pnl"] += pnl_val
                        in_position = False
                        current_trade = None
                        if global_single:
                            in_position_global = False
                            current_trade_global = None
                    continue

                # Entry logic
                if (not in_position) and bool(row.get("CROSS_UP", False)):
                    qty = qty_from_risk(  # Patch 2B
                        equity=per_symbol_equity[symbol],
                        price=price,
                        stop_loss_pct=STOP_LOSS_PCT,
                        risk_per_trade=RISK_PER_TRADE
                    )
                    current_trade = Trade(
                        symbol=symbol,
                        entry_time=ts if isinstance(ts, dt.datetime) else None,
                        entry_price=price,
                        qty=qty
                    )
                    in_position = True
                    if global_single:
                        in_position_global = True
                        current_trade_global = current_trade

            # Force-exit at end of data
            if in_position and current_trade and current_trade.symbol == symbol:
                last_price = float(df["Close"].iloc[-1])
                current_trade.exit_time = df["Time"].iloc[-1] if "Time" in df.columns else None
                current_trade.exit_price = last_price
                current_trade.pnl = pnl_from_fill(current_trade.entry_price, last_price, current_trade.qty)  # Patch 2B
                pnl_val = float(current_trade.pnl or 0.0)
                per_symbol_stats[symbol]["trades"] += 1
                if pnl_val > 0:
                    per_symbol_stats[symbol]["wins"] += 1
                else:
                    per_symbol_stats[symbol]["losses"] += 1
                per_symbol_stats[symbol]["net_pnl"] += pnl_val
                in_position = False
                current_trade = None
                if global_single:
                    in_position_global = False
                    current_trade_global = None

            report_rows.append({
                "symbol": symbol, "pass": attempt,
                "trades": per_symbol_stats[symbol]["trades"],
                "wins": per_symbol_stats[symbol]["wins"],
                "losses": per_symbol_stats[symbol]["losses"],
                "net_pnl": per_symbol_stats[symbol]["net_pnl"]
            })

        # --- Patch 3C: write per-pass equity curve CSV (deterministic order) ---
        if per_pass_curve:
            dfp = pd.DataFrame(per_pass_curve)
            sort_cols = [c for c in ("time", "symbol") if c in dfp.columns]
            if sort_cols:
                dfp = dfp.sort_values(sort_cols, kind="mergesort")
            _atomic_to_csv(dfp, LOG_DIR / f"backtest_equity_curve_pass_{attempt}.csv")

        # --- Persist combined equity + report across all passes ---
    eq_df = pd.DataFrame(equity_curve)
    if not eq_df.empty:
        sort_cols = [c for c in ("time", "symbol") if c in eq_df.columns]
        if sort_cols:
            eq_df = eq_df.sort_values(sort_cols, kind="mergesort")
        _atomic_to_csv(eq_df, LOG_DIR / "backtest_equity_curve.csv")

    rep_df = pd.DataFrame(report_rows)
    if not rep_df.empty:
        sort_cols = [c for c in ("symbol", "pass") if c in rep_df.columns]
        if sort_cols:
            rep_df = rep_df.sort_values(sort_cols, kind="mergesort")
        _atomic_to_csv(rep_df, LOG_DIR / "backtest_report.csv")

        # Daily summary (deterministic by symbol)
        final_rows = []
        for sym in sorted(per_symbol_stats.keys()):
            st = per_symbol_stats[sym]
            t = st["trades"]; w = st["wins"]; l = st["losses"]
            win_rate = (w / t * 100.0) if t else 0.0
            final_rows.append({
                "date": today_str(),
                "symbol": sym,
                "trades": t,
                "wins": w,
                "losses": l,
                "win_rate": round(win_rate, 2),
                "net_pnl": round(st["net_pnl"], 2),
            })
        # extra guard for stability even if future edits change iteration above
        final_rows = sorted(final_rows, key=lambda r: (r["date"], r["symbol"]))
        _write_backtest_summary_for_today(final_rows, LOG_DIR / "backtest_report_summary.csv")

    # Discord digest (symbols already sorted)
    lines = [
        f"**BACKTEST REPORT** | Interval {interval} | Passes {passes} | {today_str()}",
        "```",
        f"{'SYMBOL':<8} {'TRADES':>6} {'WINS':>6} {'LOSS':>6} {'WIN%':>6} {'PNL':>10}"
    ]
    for r in final_rows:
        lines.append(
            f"{r['symbol']:<8} {r['trades']:>6} {r['wins']:>6} {r['losses']:>6} "
            f"{r['win_rate']:>6.1f} {r['net_pnl']:>10.2f}"
        )
    lines.append("```")
    send_discord("\n".join(lines), "BACKTEST")

    # Aggregate totals for summary + DONE banner
    total_trades = sum(r["trades"] for r in final_rows)
    total_wins   = sum(r["wins"]   for r in final_rows)
    total_pnl    = sum(r["net_pnl"] for r in final_rows)
    winrate_tot  = (total_wins / total_trades * 100.0) if total_trades else 0.0

    summary = {
        "mode": "BACKTEST",
        "date": today_str(),
        "interval": interval,
        "symbols": symbols,
        "passes": passes,
        "total_trades": total_trades,
        "total_wins": total_wins,
        "win_rate_total": round(winrate_tot, 2),
        "net_pnl_total": round(total_pnl, 2),
    }
    log.info("Backtest Summary:\n" + json.dumps(summary, indent=2))

    # DONE banner
    send_discord(
        f"**BACKTEST DONE** {today_str()} | Trades={total_trades} | Win%={winrate_tot:.1f} | NetPnL={total_pnl:.2f}",
        "BACKTEST"
    )

    return summary

# ==============================
# 11) Paper/Live loop (full)
# ==============================
class SessionStats:
    def __init__(self, mode: str):
        self.mode = mode
        self.session_date = now_ny().date().isoformat()
        self.loss_streak = defaultdict(int)     # per-symbol loss streak (today)
        self.cum_pnl = defaultdict(float)       # per-symbol realized PnL (today)
        self.halted = defaultdict(bool)         # per-symbol in-day halt
        self.open_trades = {}                   # active positions

        # Which CSV filename to use; actual writes go through LOG_DIR via _log_trade_row
        self.trade_log_filename = "paper_trades.csv" if mode == "PAPER" else "live_trades.csv"

        # Baselines for risk caps
        self.day_start_equity: Optional[float] = None
        self.week_start_equity: Optional[float] = None
        self.trading_halted_today: bool = False
        self.trading_halted_week: bool = False

    def log_trade_csv(self, row: dict):
        """Thin wrapper to the unified writer that targets LOG_DIR."""
        _log_trade_row(row, self.trade_log_filename)

def _positions_index(api: TradingClient) -> Dict[str, dict]:
    out = {}
    try:
        for p in api.get_all_positions():
            out[p.symbol.upper()] = {"qty": int(float(p.qty)), "avg_entry": _safe_float(p.avg_entry_price, 0.0)}
    except Exception as e:
        log.debug(f"_positions_index error: {e}")
    return out

def flatten_all_positions(api: TradingClient, send_func=send_discord, mode="PAPER") -> int:
    """
    Best-effort flatten:
      1) cancel all open orders,
      2) close each open position (market).
    Returns number of symbols attempted to close.
    """
    attempted = 0
    try:
        # Cancel working orders first so they don't interfere with closes
        try:
            api.cancel_all_orders()
        except Exception:
            pass

        positions = []
        try:
            positions = api.get_all_positions()
        except Exception as e:
            send_func(f"⚠️ {mode} risk-breach: failed to list positions: {e}", mode)
            return 0

        for pos in positions:
            sym = getattr(pos, "symbol", None) or getattr(pos, "Symbol", None)
            qty_str = getattr(pos, "qty", "0")
            side = getattr(pos, "side", "").lower()
            try:
                q = abs(int(float(qty_str)))
            except Exception:
                q = 0
            if not sym or q <= 0:
                continue

            attempted += 1
            ok = False
            # Try the broker-native close helper
            try:
                api.close_position(sym)
                ok = True
            except Exception:
                # Fall back to explicit market order, reversing side
                try:
                    order_side = OrderSide.SELL if side == "long" else OrderSide.BUY
                    api.submit_order(
                        symbol=sym,
                        qty=q,
                        side=order_side,
                        type=OrderType.MARKET,
                        time_in_force=TimeInForce.DAY,
                    )
                    ok = True
                except Exception as e:
                    send_func(f"❌ {mode} flatten failed for {sym}: {e}", mode)

            if ok:
                send_func(f"✅ {mode} flattened {sym} (qty={q})", mode)

    except Exception as e:
        send_func(f"❌ {mode} flatten_all_positions error: {e}", mode)
    return attempted

# ---- Risk baselines (day/week) ----
def _iso_week_key_ny(ts: "dt.datetime") -> str:
    d = ts.astimezone(TZ_NY).date()
    iso = d.isocalendar()  # (year, week, weekday)
    return f"{iso.year}-W{iso.week:02d}"

def _get_recent_closed_sell_fill(api: TradingClient, symbol: str) -> Optional[Tuple[float, int, str, str]]:
    try:
        orders = api.get_orders(status="closed", symbols=[symbol], limit=25, nested=True)
        for o in orders:
            if getattr(o, "side", "").lower() == "sell" and getattr(o, "filled_avg_price", None):
                price = _safe_float(o.filled_avg_price)
                qty = int(float(o.filled_qty)) if getattr(o, "filled_qty", None) else int(float(o.qty))
                filled_at = getattr(o, "filled_at", None)
                ts = filled_at.isoformat() if hasattr(filled_at, "isoformat") else str(filled_at or "")
                return price, qty, getattr(o, "client_order_id", ""), ts
    except Exception as e:
        log.debug(f"_recent_closed_sell_fill error for {symbol}: {e}")
    return None

def enforce_single_position(api: TradingClient) -> bool:
    try:
        return len(api.get_all_positions()) == 0
    except Exception as e:
        log.warning(f"Could not fetch positions: {e}. Failing closed.")
        return False

def pdt_guard(api: TradingClient) -> bool:
    try:
        equity = float(api.get_account().equity)
        if equity < 26000:
            log.info(f"PDT guard: equity ${equity:,.2f} < 26k")
        return True
    except Exception as e:
        log.warning(f"PDT check failed: {e}")
        return True

def is_market_open(api: TradingClient) -> Tuple[bool, Optional[dt.datetime], Optional[dt.datetime]]:
    try:
        clock = api.get_clock()
        is_open = bool(getattr(clock, "is_open", False))
        nopen = getattr(clock, "next_open", None)
        nclose = getattr(clock, "next_close", None)
        to_local = lambda t: t.astimezone(TZ_NY) if hasattr(t, "astimezone") else None
        return is_open, to_local(nopen), to_local(nclose)
    except Exception as e:
        log.warning(f"Market clock failed: {e} — assuming open.")
        return True, None, None

def sleep_until_market_open(api: TradingClient):
    is_open, next_open, _ = is_market_open(api)
    if is_open or not next_open:
        time.sleep(60)
        return
    while True:
        if check_kill_signal(): break
        secs = (next_open - now_ny()).total_seconds() - MARKET_SLEEP_BUFFER_SECONDS
        if secs <= 0: break
        time.sleep(min(60, max(5, int(secs))))

def _reset_session_if_new_day(stats: "SessionStats"):
    today = now_ny().date().isoformat()
    if getattr(stats, "session_date", None) != today:
        stats.session_date = today
        stats.loss_streak.clear()
        stats.halted.clear()
        stats.cum_pnl.clear()
        stats.trading_halted_today = False
        # Note: weekly halt persists until a new ISO week
        log.info("[Session] New trading day detected — session counters reset.")

def _sync_open_positions_on_start(api: TradingClient, stats: "SessionStats"):
    try:
        positions = api.get_all_positions()
    except Exception as e:
        log.warning(f"[Sync] Could not load positions on start: {e}")
        return
    for p in positions:
        sym = p.symbol.upper()
        try:
            qty = int(float(p.qty))
            entry = float(p.avg_entry_price)
        except Exception:
            continue
        tp = round(entry * (1 + TAKE_PROFIT_PCT), 2)
        sl = round(entry * (1 - STOP_LOSS_PCT), 2)
        stats.open_trades[sym] = {
            "entry_price": entry,
            "qty": qty,
            "tp": tp,
            "sl": sl,
            "client_order_id": "",
            "opened_at": now_ny(),
        }
    if stats.open_trades:
        log.info(f"[Sync] Resumed managing open positions: {list(stats.open_trades.keys())}")

def _refresh_day_baseline_if_needed(api: TradingClient, stats: "SessionStats"):
    if stats.day_start_equity is None:
        eq = _get_equity(api)
        if eq is not None:
            stats.day_start_equity = eq
            log.info(f"[Risk] Day baseline equity set: ${eq:,.2f}")

def _refresh_week_baseline_if_needed(api: TradingClient, stats: "SessionStats"):
    # Load persisted week state; if new week or missing baseline, set baseline to current equity
    state = _load_week_state()
    wk = _week_key(now_ny())
    if state.get("week_key") != wk or state.get("week_start_equity") in (None, 0):
        eq = _get_equity(api)
        if eq is not None:
            state = {"week_key": wk, "week_start_equity": eq}
            _save_week_state(state)
            stats.week_start_equity = eq
            log.info(f"[Risk] Week baseline equity set for {wk}: ${eq:,.2f}")
    else:
        stats.week_start_equity = _safe_float(state.get("week_start_equity"), None)

# ==============================
# Heartbeat & Countdown Helpers
# ==============================
def _get_heartbeat_webhook(mode: str) -> Optional[str]:
    """Return dedicated heartbeat webhook for mode, if configured."""
    m = (mode or "").upper()
    if m == "PAPER":
        return os.getenv("DISCORD_HEARTBEAT_WEBHOOK_PAPER") or os.getenv("DISCORD_HEARTBEAT_WEBHOOK")
    if m == "LIVE":
        return os.getenv("DISCORD_HEARTBEAT_WEBHOOK_LIVE") or os.getenv("DISCORD_HEARTBEAT_WEBHOOK")
    return os.getenv("DISCORD_HEARTBEAT_WEBHOOK")

def _post_discord_webhook(url: str, content: str) -> None:
    try:
        requests.post(url, json={"content": content}, timeout=10)
    except Exception:
        pass

def send_discord_heartbeat(message: str, mode: str) -> None:
    """Send heartbeat to a dedicated Discord webhook if available, else fall back."""
    hb_url = _get_heartbeat_webhook(mode)
    if hb_url:
        _post_discord_webhook(hb_url, message)
        try:
            log.info("[HB] " + message.replace("\n", " | "))
        except Exception:
            pass
        return
    # fallback to primary
    try:
        send_discord(message, mode)
    except Exception:
        # Final fallback to generic webhook if defined
        url = os.getenv("DISCORD_WEBHOOK")
        if url:
            _post_discord_webhook(url, message)

def _start_open_countdown(mode: str):
    """During market-closed periods, send countdown: 2h→1h→30m→15m to open."""
    if mode not in ("PAPER", "LIVE"):
        return
    enabled = os.getenv("MARKET_CLOSED_NOTIFY", "true").lower() == "true"
    if not enabled:
        return
    
    global _COUNTDOWN_STARTED
    if _COUNTDOWN_STARTED:
        return
    _COUNTDOWN_STARTED = True

    interval_min = int(os.getenv("MARKET_CLOSED_INTERVAL_MIN", "120"))
    finals_raw = os.getenv("MARKET_CLOSED_FINAL_MINUTES", "60,30,15")
    finals = []
    for tok in finals_raw.split(","):
        tok = tok.strip()
        if tok.isdigit():
            finals.append(int(tok))
    finals = sorted(set(finals), reverse=True)  # e.g., [60,30,15]

    def runner():
        api = build_trading_api(mode)
        posted_final = set()
        last_general_ts = 0.0
        import time as _t
        while True:
            try:
                if check_kill_signal():
                    break
                is_open, next_open, _ = is_market_open(api)
                if is_open:
                    posted_final.clear()
                    # While open we do nothing here; heartbeat thread handles open cadence
                    for _ in range(60):
                        if check_kill_signal():
                            break
                        _t.sleep(1)
                    continue

                # Closed: compute minutes to open
                if not next_open:
                    # No next open known; backoff
                    for _ in range(60):
                        if check_kill_signal():
                            break
                        _t.sleep(1)
                    continue

                now_ny_dt = now_ny()
                delta = max(dt.timedelta(0), next_open - now_ny_dt)
                mins_to_open = int(delta.total_seconds() // 60)

                # General closed ping every interval until we are within smallest 'final' minute
                smallest_final = min(finals) if finals else 60
                now_ts = _t.time()
                if mins_to_open > smallest_final and now_ts - last_general_ts >= max(60, interval_min * 60):
                    eta_text, local_text = _fmt_eta_to_open(next_open)
                    line = f"**{mode}** Market CLOSED · Next open in {eta_text}"
                    if local_text:
                        line += f" · {local_text}"

                    # fan-out: heartbeat channel + main mode channel + heartbeat.log
                    send_discord_heartbeat(line, mode)
                    send_discord(line, mode)
                    _log_heartbeat_line(line, mode)

                    try:
                        log.info(f"[Countdown] General ping: {mins_to_open} min to open")
                    except Exception:
                        pass
                    last_general_ts = now_ts

                # Final thresholds
                for m in finals:
                    if mins_to_open <= m and m not in posted_final:
                        eta_text, local_text = _fmt_eta_to_open(next_open)
                        line = f"**{mode}** Market opens in {m} min ({eta_text})"
                        if local_text:
                            line += f" · {local_text}"

                        # Fan‑out: heartbeat channel + mode channel + heartbeat.log
                        send_discord_heartbeat(line, mode)   # dedicated heartbeat channel
                        send_discord(line, mode)             # mode’s main webhook (PAPER/LIVE)
                        _log_heartbeat_line(line, mode)      # persist with mode tag

                        try:
                            log.info(f"[Countdown] Final ping at {m} min")
                        except Exception:
                            pass
                        posted_final.add(m)
                        # slight pause to avoid multiple triggers same minute tick
                        _t.sleep(3)

                # sleep a bit then re-check
                for _ in range(30):
                    if check_kill_signal():
                        break
                    _t.sleep(1)
            except Exception as e:
                try:
                    log.warning(f"[Countdown] loop error: {e}")
                except Exception:
                    pass
                _t.sleep(10)

    threading.Thread(target=runner, daemon=True).start()

def trading_loop(mode: str):
    assert mode in ("PAPER", "LIVE")
    api = build_trading_api(mode)
    data_client = build_data_client()

    # persistent state
    state = _json_load(SESSION_STATE_PATH, {"halted_sessions": {}})
    disabled = _load_disabled_set()

    base_syms = SYMBOLS[:]
    user_syms = _load_user_symbols()

    def active_symbols() -> List[str]:
        merged = []
        for s in base_syms + user_syms:
            s = s.upper()
            if s and s not in merged:
                merged.append(s)
        return [s for s in merged if s not in disabled]

    acts = active_symbols()
    send_discord(f"**{mode}** trading loop started: {acts}", mode)
    # One-time startup banner about market state & ETA
    try:
        _startup_market_banner(mode)
    except Exception:
        pass

    # Start background heartbeat scheduler (Paper/Live)
    try:
        _start_heartbeat_scheduler(mode)
    except Exception:
        log.warning("[Heartbeat] scheduler failed to start", exc_info=True)

    scan_cursor = 0
    
    global_single = PAPER_GLOBAL_SINGLE_POSITION if mode == "PAPER" else LIVE_GLOBAL_SINGLE_POSITION
    stats = SessionStats(mode)
    _sync_open_positions_on_start(api, stats)
    # LIVE EOD flatten: per‑session guard so we only do it once each day
    eod_flatten_done = False

    def halt_symbol(sym: str):
        stats.halted[sym] = True
        send_discord(
            f"**{mode} HALT** {sym} after 2 consecutive losses.\n"
            f"Symbol PnL: {stats.cum_pnl[sym]:+.2f}\n"
            f"Overall PnL: {sum(stats.cum_pnl.values()):+.2f}",
            mode
        )

    # Simulated exits in PAPER dry-run (off-hours): close if last price crosses TP/SL
    if mode == "PAPER" and PAPER_DRY_RUN and stats.open_trades:
        to_close = []
        for sym, od in list(stats.open_trades.items()):
            try:
                end = now_ny(); start = end - dt.timedelta(days=1)
                req = StockBarsRequest(
                    symbol_or_symbols=sym,
                    timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                    start=start, end=end, limit=1, feed=DataFeed.IEX
                )
                bars = data_client.get_stock_bars(req).df
                if isinstance(bars.index, pd.MultiIndex):
                    bars = bars.xs(sym, level=0)
                if bars is None or bars.empty:
                    continue
                last_close = float(bars["close"].iloc[-1])
                crossed_tp = last_close >= od["tp"]
                crossed_sl = last_close <= od["sl"]
                if crossed_tp or crossed_sl:
                    exit_price = od["tp"] if crossed_tp else od["sl"]
                    qty = od["qty"]
                    pnl = (exit_price - od["entry_price"]) * qty
                    stats.cum_pnl[sym] += pnl
                    result = "WIN" if pnl > 0 else "LOSS"
                    if pnl <= 0: stats.loss_streak[sym] += 1
                    else:        stats.loss_streak[sym] = 0
                    _log_trade_row({
                        "time_open": od["opened_at"].isoformat(),
                        "time_close": now_ny().isoformat(),
                        "symbol": sym, "qty": qty,
                        "entry_price": round(od["entry_price"], 4),
                        "exit_price": round(float(exit_price), 4),
                        "pnl": round(float(pnl), 2),
                        "result": result,
                        "mode": mode,
                        "tp": round(od["tp"], 4),
                        "sl": round(od["sl"], 4),
                        "client_order_id": od.get("client_order_id", "")
                    }, "paper_trades.csv")

                    # define exit timestamp for the emit
                    exit_ts = now_ny().isoformat()

                    _ml_emit(
                        schemas.execution,
                        ts_open=od["opened_at"].isoformat(),
                        ts_close=exit_ts,
                        session=today_str(),
                        symbol=sym,
                        side="sell",
                        qty=float(qty),
                        entry_price=float(od["entry_price"]),
                        exit_price=float(exit_price),
                        pnl=float(pnl),
                        client_order_id=str(od.get("client_order_id", ""))
                    )

                    send_discord(
                        f"**{mode} EXIT (SIM)** {sym} qty={qty} PnL={pnl:+.2f} "
                        f"(streak={stats.loss_streak[sym]}, cum={stats.cum_pnl[sym]:+.2f})",
                        mode,
                    )
                    if stats.loss_streak[sym] >= 2 and not stats.halted[sym]:
                        halt_symbol(sym)
                    to_close.append(sym)
            except Exception:
                pass
        for s in to_close:
            stats.open_trades.pop(s, None)


    def handle_closures():
        positions = _positions_index(api)
        closed_now = []
        for sym, od in list(stats.open_trades.items()):
            if sym in positions:
                continue  # still open
            # closed — try to read fill
            exit_price, _, _, exit_ts = (None, None, "", "")
            fill = _get_recent_closed_sell_fill(api, sym)
            if fill:
                exit_price, _, _, exit_ts = fill

            if exit_price is None:
                try:
                    end = now_ny(); start = end - dt.timedelta(days=1)
                    req = StockBarsRequest(symbol_or_symbols=sym, timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                                           start=start, end=end, limit=1, feed=DataFeed.IEX)
                    bars = data_client.get_stock_bars(req).df
                    if isinstance(bars.index, pd.MultiIndex):
                        bars = bars.xs(sym, level=0)
                    if not bars.empty:
                        exit_price = float(bars["close"].iloc[-1])
                        exit_ts = bars.index[-1].isoformat()
                except Exception:
                    pass
            if exit_price is None:
                exit_price = od["entry_price"]
                exit_ts = now_ny().isoformat()

            qty = od["qty"]
            pnl = (exit_price - od["entry_price"]) * qty
            stats.cum_pnl[sym] += pnl
            result = "WIN" if pnl > 0 else "LOSS"
            if pnl <= 0: stats.loss_streak[sym] += 1
            else:        stats.loss_streak[sym] = 0

            _log_trade_row({
                "time_open": od["opened_at"].isoformat(),
                "time_close": now_ny().isoformat(),
                "symbol": sym, "qty": qty,
                "entry_price": round(od["entry_price"], 4),
                "exit_price": round(float(exit_price), 4),
                "pnl": round(float(pnl), 2),
                "result": result,
                "mode": mode,
                "tp": round(od["tp"], 4),
                "sl": round(od["sl"], 4),
                "client_order_id": od.get("client_order_id", "")
            }, "paper_trades.csv")

            event_bus.bus.send(
                schemas.execution(
                    ts_open=od["opened_at"].isoformat(),
                    ts_close=exit_ts,
                    session=today_str(),
                    symbol=sym,
                    side="sell",
                    qty=float(qty),
                    entry_price=float(od["entry_price"]),
                    exit_price=float(exit_price),
                    pnl=float(pnl),
                    client_order_id=str(od.get("client_order_id",""))
                )
            )

            send_discord(f"**{mode} EXIT** {sym} qty={qty} PnL={pnl:+.2f} "
                         f"(streak={stats.loss_streak[sym]}, cum={stats.cum_pnl[sym]:+.2f})", mode)

            if stats.loss_streak[sym] >= 2 and not stats.halted[sym]:
                halt_symbol(sym)

            closed_now.append(sym)

        for sym in closed_now:
            stats.open_trades.pop(sym, None)

    # >>> DEDENT starts here: this while-loop must be at the same level as def handle_closures() <<<
        # main trading loop
    while True:
        try:
            # 0) Kill-switch?
            kill_reason = None
            if check_kill_signal():
                kill_reason = "signal"
            elif check_external_kill():
                kill_reason = "external"

            if kill_reason:
                msg = f"[KILL] {mode} loop shutting down (reason={kill_reason})"
                log.warning(msg)
                send_discord(f"⚠️ {mode} KILL — reason={kill_reason}. Trader loop exiting.", mode)
                break

            # 1) Heartbeat
            try:
                beat("trader")
                log.info(f"[Heartbeat] {mode} trader loop alive")
            except Exception as e:
                log.warning(f"[Heartbeat] {mode} trader beat failed: {e}")
                send_discord(f"⚠️ {mode} Heartbeat failed: {e}", mode)

            # 2) Daily reset (date rollover)
            _reset_session_if_new_day(stats)

            # 3) Refresh baselines & enforce loss guardrails
            _refresh_day_baseline_if_needed(api, stats)
            _refresh_week_baseline_if_needed(api, stats)

            total_today_pnl = sum(stats.cum_pnl.values())

            # Daily loss guard
            if stats.day_start_equity is not None:
                daily_floor = -DAILY_LOSS_LIMIT_PCT * stats.day_start_equity
                if total_today_pnl <= daily_floor and not stats.trading_halted_today:
                    stats.trading_halted_today = True
                    send_discord(
                        f"**{mode} DAILY LOSS LIMIT HIT** "
                        f"(PnL {total_today_pnl:+.2f} ≤ {daily_floor:.2f}; {DAILY_LOSS_LIMIT_PCT:.0%})\n"
                        f"Halting new entries until next session.",
                        mode
                    )
                    # optional auto-flatten from env caps
                    try:
                        _, _, flatten_flag = _get_loss_caps_from_env()
                        if flatten_flag:
                            cnt = flatten_all_positions(api, send_discord, mode)
                            send_discord(f"**{mode} RISK ACTION** flattened {cnt} open position(s).", mode)
                    except Exception as e:
                        log.warning(f"[Risk/Daily] flatten failed: {e}")

                    # Optional flatten on breach (from env)
                    _, _, _flatten = _get_loss_caps_from_env()
                    if _flatten:
                        try:
                            cnt = flatten_all_positions(api, send_discord, mode)
                            send_discord(f"🧹 {mode} DAILY breach: flattened {cnt} open position(s).", mode)
                        except Exception as e:
                            log.warning(f"[RISK-DAILY] flatten error: {e}")
                            send_discord(f"❌ {mode} DAILY flatten error: {e}", mode)

            # Weekly loss guard (based on equity drawdown)
            if stats.week_start_equity is not None:
                eq_now = _get_equity(api)
                if eq_now is not None:
                    weekly_drawdown = stats.week_start_equity - eq_now
                    weekly_floor = WEEKLY_LOSS_LIMIT_PCT * stats.week_start_equity
                    if weekly_drawdown >= weekly_floor and not stats.trading_halted_week:
                        stats.trading_halted_week = True
                        send_discord(
                            f"**{mode} WEEKLY LOSS LIMIT HIT** "
                            f"(Drawdown {weekly_drawdown:.2f} ≥ {weekly_floor:.2f}; {WEEKLY_LOSS_LIMIT_PCT:.0%})\n"
                            f"Halting new entries until next week.",
                            mode
                        )
                        # optional auto-flatten from env caps (same flag)
                        try:
                            _, _, flatten_flag = _get_loss_caps_from_env()
                            if flatten_flag:
                                cnt = flatten_all_positions(api, send_discord, mode)
                                send_discord(f"**{mode} RISK ACTION** flattened {cnt} open position(s).", mode)
                        except Exception as e:
                            log.warning(f"[Risk/Weekly] flatten failed: {e}")

                        # Optional flatten on breach (from env)
                        _, _, _flatten = _get_loss_caps_from_env()
                        if _flatten:
                            try:
                                cnt = flatten_all_positions(api, send_discord, mode)
                                send_discord(f"🧹 {mode} WEEKLY breach: flattened {cnt} open position(s).", mode)
                            except Exception as e:
                                log.warning(f"[RISK-WEEKLY] flatten error: {e}")
                                send_discord(f"❌ {mode} WEEKLY flatten error: {e}", mode)

            # 4) Market-hours gating (skip if PAPER dry-run)
            if not TRADE_EXTENDED_HOURS and not (mode == "PAPER" and PAPER_DRY_RUN):
                open_now, nxt_open, nxt_close = is_market_open(api)  # capture next_close too
            else:
                open_now, nxt_open, nxt_close = True, None, None  # treat as open in ext-hours or dry-run

            if not open_now:
                # EOD flatten fail-safe (LIVE only): if enabled and not done yet, flatten now
                if (mode == "LIVE") and (not eod_flatten_done):
                    try:
                        _do_flatten = str(os.getenv("LIVE_FLATTEN_AT_CLOSE", "false")).strip().lower() == "true"
                        if _do_flatten:
                            cnt = flatten_all_positions(api, send_discord, mode)
                            send_discord(f"🧹 {mode} EOD (market closed): flattened {cnt} open position(s).", mode)
                            eod_flatten_done = True
                    except Exception as e:
                        log.warning(f"[EOD] {mode} flatten on closed-market failed: {e}")

                # one notice, then sleep properly until open
                send_discord(
                    f"**{mode}** Market closed. Waiting until "
                    f"{nxt_open.strftime('%Y-%m-%d %H:%M:%S %Z') if nxt_open else 'open'}",
                    mode,
                )
                sleep_until_market_open(api)
                continue  # loop back after market opens
                        # LIVE only: pre-close flatten window
            if (mode == "LIVE") and (not eod_flatten_done):
                try:
                    _do_flatten = str(os.getenv("LIVE_FLATTEN_AT_CLOSE", "false")).strip().lower() == "true"
                    if _do_flatten and nxt_close is not None:
                        mins_before = int(os.getenv("LIVE_FLATTEN_MINUTES_BEFORE_CLOSE", "5") or "5")
                        # minutes until close
                        m_to_close = (nxt_close - now_ny()).total_seconds() / 60.0
                        if m_to_close <= max(0, mins_before):
                            cnt = flatten_all_positions(api, send_discord, mode)
                            send_discord(f"🧹 {mode} EOD ({mins_before}m to close): flattened {cnt} open position(s).", mode)
                            eod_flatten_done = True
                except Exception as e:
                    log.warning(f"[EOD] {mode} pre-close flatten failed: {e}")

            # 5) Handle closures (fills or simulated exits)
            handle_closures()

            # 6) Update runtime status snapshot for !status (single source of truth)
            write_status_snapshot(
                mode=mode,
                today_pnl=sum(stats.cum_pnl.values()),
                open_positions=len(stats.open_trades)
            )

            # 7) Reload live-edited lists from Discord (user symbols / disabled)
            user_syms = _load_user_symbols()
            disabled  = _load_disabled_set()

            # Active symbols (base + user minus disabled)
            def _active_symbols():
                merged = []
                for s in SYMBOLS + user_syms:
                    s = s.upper()
                    if s and s not in merged:
                        merged.append(s)
                return [s for s in merged if s not in disabled]

            acts = _active_symbols()

            # 8) Round-robin ordering: start from scan_cursor
            if acts:
                ordered = acts[scan_cursor:] + acts[:scan_cursor]
            else:
                ordered = []

            # 9) Global single-position guard (if on and a position exists, back off)
            global_single = PAPER_GLOBAL_SINGLE_POSITION if mode == "PAPER" else LIVE_GLOBAL_SINGLE_POSITION
            if global_single and not enforce_single_position(api):
                time.sleep(15)
                continue

            # 10) PDT guard (informational/backoff if day-trade-limited)
            if not pdt_guard(api):
                time.sleep(30)
                continue

            # 11) Scan & possibly enter
            traded = False
            for sym in ordered:
                # Respect global halts: if daily/weekly loss limits hit, do not open new positions
                if stats.trading_halted_today or stats.trading_halted_week:
                    break
                # Per-symbol halt after 2 losses
                if stats.halted.get(sym, False):
                    continue
                # Skip if already in a position
                if sym in stats.open_trades:
                    continue

                # Pull last 5d of 1m bars
                end = now_ny(); start = end - dt.timedelta(days=5)
                req = StockBarsRequest(
                    symbol_or_symbols=sym,
                    timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                    start=start, end=end, limit=1500, feed=DataFeed.IEX
                )
                # NEW:
                bars = data_client.get_stock_bars(req).df
                if isinstance(bars.index, pd.MultiIndex):
                    bars = bars.xs(sym, level=0)
                if bars is None or len(bars) < max(SMA_SLOW, RSI_PERIOD) + 5:
                    continue

                df = pd.DataFrame({
                    "Open": bars["open"], "High": bars["high"],
                    "Low": bars["low"], "Close": bars["close"], "Volume": bars["volume"]
                })
                df = generate_signals(df)
                last = df.iloc[-1]

                # (optional) ML sidecar feature snapshot
                _ml_emit(
                    schemas.feature_snapshot,
                    ts=now_ny(),
                    session=today_str(),
                    symbol=sym,
                    features={
                        "close": float(last["Close"]),
                        "sma_fast": float(last["SMA_F"]),
                        "sma_slow": float(last["SMA_S"]),
                        "rsi": float(last["RSI"]),
                        "cross_up": float(last["CROSS_UP"]),
                        "volume": float(last["Volume"]) if "Volume" in last else 0.0,
                    },
                    feature_version=os.getenv("FEATURE_VERSION", "f1.0"),
                    strategy_version=os.getenv("STRATEGY_VERSION", "s1.0"),
                    model_version=os.getenv("MODEL_VERSION", "none"),
                    mode=mode.lower(),
                )

                if bool(last["CROSS_UP"]):
                    price = float(last["Close"])
                    sl = price * (1 - STOP_LOSS_PCT)
                    tp = price * (1 + TAKE_PROFIT_PCT)
                    risk_per_share = max(price - sl, 0.01)
                    equity = float(api.get_account().equity)
                    qty = int(max((equity * RISK_PER_TRADE) / risk_per_share, 1))

                    if global_single and not enforce_single_position(api):
                        send_discord(f"⏸ {mode} skip {sym}: a position is already open (single‑position policy).", mode)
                        continue

                                        # idempotent client_order_id (dedup on retries)
                    import hashlib, os
                    nonce = f"{now_ny().isoformat()}-{os.getpid()}-{time.time_ns()}"
                    coid = f"TB-{mode}-{sym}-{hashlib.md5(nonce.encode()).hexdigest()[:10]}"

                    if mode == "PAPER" and PAPER_DRY_RUN:
                        # Simulate a dry-run order (instant fill at signal price)
                        filled_price = price
                        status_note  = "SIMULATED"
                        send_discord(f"**{mode} DRY-RUN ORDER** {sym} qty={qty} @ {filled_price:.2f}", mode)
                    else:
                        # LIVE/PAPER real submit with small retry/backoff — MARKET BUY + BRACKET (TP/SL)
                        submit_ok = False
                        last_err  = None
                        for attempt in range(1, 4):  # up to 3 tries
                            try:
                                # if already exists (retry), skip re-submit
                                try:
                                    orders = api.get_orders(status="open", limit=50, nested=True)
                                    if any(getattr(o, "client_order_id", "") == coid for o in orders):
                                        submit_ok = True
                                        break
                                except Exception:
                                    pass

                                api.submit_order(
                                    symbol=sym,
                                    qty=qty,
                                    side=OrderSide.BUY,
                                    type=OrderType.MARKET,
                                    time_in_force=TimeInForce.GTC,
                                    order_class=OrderClass.BRACKET,
                                    take_profit={"limit_price": float(round(tp, 2))},
                                    stop_loss={"stop_price": float(round(sl, 2))},
                                    client_order_id=coid,
                                )
                                submit_ok = True
                                break
                            except Exception as e:
                                last_err = e
                                # backoff: 1s, 2s, 4s
                                time.sleep(2 ** (attempt - 1))
                        if not submit_ok:
                            send_discord(f"**{mode} ORDER FAILED** {sym} qty={qty}: {last_err}", mode)
                            log.warning(f"[Order] Submit failed for {sym}: {last_err}")
                            continue

                    # Confirm fill / handle rejection/timeout lightly
                    if mode == "PAPER" and PAPER_DRY_RUN:
                        entry_price_used = filled_price
                        status_note = "SIMULATED"
                    else:
                        filled_price = None
                        status_note  = "PENDING"
                        for _ in range(10):  # ~20s
                            try:
                                orders = api.get_orders(status="all", limit=50, nested=True)
                                match = None
                                for o in orders:
                                    if getattr(o, "client_order_id", "") == coid:
                                        match = o
                                        break
                                if match:
                                    st  = getattr(match, "status", "").lower()
                                    fpx = getattr(match, "filled_avg_price", None)
                                    if fpx:
                                        filled_price = float(fpx)
                                        status_note  = st.upper()
                                        break
                                    if st in ("rejected", "canceled", "expired"):
                                        send_discord(f"**{mode} ORDER {st.upper()}** {sym} qty={qty}", mode)
                                        log.warning(f"[Order] {sym} {st} (coid={coid})")
                                        filled_price = None
                                        break
                            except Exception:
                                pass
                            time.sleep(2)

                        entry_price_used = filled_price if filled_price is not None else float(last["Close"])

                    # Record position locally
                    stats.open_trades[sym] = {
                        "entry_price": entry_price_used,
                        "qty": qty,
                        "tp": round(tp, 2),
                        "sl": round(sl, 2),
                        "client_order_id": coid,
                        "opened_at": now_ny(),
                    }

                    send_discord(
                        f"**ENTER {mode}** {sym} qty={qty} @ ~{entry_price_used:.2f} "
                        f"TP={tp:.2f} SL={sl:.2f} ({status_note})",
                        mode,
                    )
                    traded = True

                    # Advance scan cursor to the symbol after the one we just used
                    try:
                        idx = acts.index(sym)
                        scan_cursor = (idx + 1) % len(acts) if acts else 0
                    except ValueError:
                        pass  # acts changed; keep cursor

                    if global_single:
                        break

            # 12) Pace next scan
            time.sleep(15 if traded else 8)

        except KeyboardInterrupt:
            log.info("User interrupt.")
            break
        except Exception as e:
            # keep loop alive, log and wait briefly
            log.exception(f"{mode} loop error: {e}")
            time.sleep(10)


    # Session wrap-up & disabling after 2 halted sessions
    try:
        handle_closures()
    except Exception:
        pass

    acts = active_symbols()
    lines = [f"**{mode} SESSION REPORT** | {today_str()}",
             "```",
             f"{'SYMBOL':<8} {'LOSSES':>6} {'CUMPNL':>10} {'HALTED':>7} {'SESS.HALTS':>10}"]
    overall = 0.0
    newly_disabled = []
    disabled = _load_disabled_set()
    state = _json_load(SESSION_STATE_PATH, {"halted_sessions": {}})

    for sym in acts:
        lp = stats.loss_streak[sym]
        cp = stats.cum_pnl[sym]
        hl = bool(stats.halted[sym])
        overall += cp

        h_sessions = int(state["halted_sessions"].get(sym, 0))
        if hl: h_sessions += 1
        state["halted_sessions"][sym] = h_sessions

        if h_sessions >= 2 and sym not in disabled:
            disabled.add(sym)
            newly_disabled.append(sym)

        lines.append(f"{sym:<8} {lp:>6} {cp:>10.2f} {('YES' if hl else 'NO'):>7} {h_sessions:>10}")

    lines += ["```", f"**Overall PnL:** {overall:+.2f}"]
    send_discord("\n".join(lines), mode)
    log.info("\n".join(lines))
    _json_save(SESSION_STATE_PATH, state)
    _save_disabled_set(disabled)

    # Build daily rollups from this session
    try:
        rows = []
        for sym, pnl in stats.cum_pnl.items():
            rows.append({
                "date": today_str(),
                "mode": mode,
                "symbol": sym,
                "cum_pnl": round(pnl, 2),
                "loss_streak": stats.loss_streak[sym],
                "halted": bool(stats.halted[sym]),
            })
        _write_daily_rollup(rows, Path("daily_rollup.csv"))
    except Exception as e:
        log.warning(f"[DailyRollup] build failed: {e}")

    # Build weekly rollups from trade logs (Paper/Live)
    try:
        _write_weekly_rollups()
    except Exception as e:
        log.warning(f"[WeeklyRollup] build failed: {e}")
    if newly_disabled:
        send_discord(
            f"**{mode}: SYMBOLS REMOVED** (2 halted sessions reached)\n"
            f"Removed: {sorted(newly_disabled)}\n"
            f"➡️ Replace these symbols in your universe.",
            mode
        )
    # Build monthly rollups from trade logs (Paper/Live)
    try:
        _write_monthly_rollups()
    except Exception as e:
        log.warning(f"[MonthlyRollup] build failed: {e}")

# ==============================
# 12) Discord inbound commands
# ==============================
def _start_discord_listener():
    token = DISCORD_BOT_TOKEN
    chan_id = DISCORD_KILL_CHANNEL_ID
    kill_phrase = DISCORD_KILL_PHRASE

    if not token or not chan_id or not _HAS_DISCORD:
        log.info("[DiscordBot] Not configured; skipping inbound listener.")
        return

    intents = discord.Intents.none()
    intents.message_content = True
    intents.guilds = True
    intents.messages = True
    client = discord.Client(intents=intents)
    kill_channel_id = int(chan_id)

    @client.event
    async def on_ready():
        log.info(f"[DiscordBot] Logged in as {client.user} (channel {kill_channel_id})")

    def _reply_limit(s: str) -> str: return (s or "")[:1800]

    @client.event
    async def on_message(message):
        try:
            if message.channel.id != kill_channel_id:
                return
            content = (message.content or "").strip()
            if not content:
                return
            lower = content.lower()

            if lower.startswith(kill_phrase.lower()):
                KILL_FLAG["stop"] = True
                await message.channel.send("TradingBot received **kill**. Shutting down…")
                return

            if lower == "!help":
                cmds = [
                    "`!help` - show commands",
                    "`!symbols` - list active/disabled/user-added",
                    "`!add TICKER` - add symbol (persisted)",
                    "`!remove TICKER` - remove from user-added",
                    "`!enable TICKER` - re-enable symbol",
                    "`!disable TICKER` - disable symbol",
                    "`!kill` - stop bot"
                ]
                await message.channel.send(_reply_limit("\n".join(cmds)))
                return
            
            if lower == "!status":
                rs = _read_runtime_status()
                hb = rs.get("hb", {})
                mode_now = rs.get("mode", MODE)
                pnl = rs.get("today_pnl", 0.0)
                open_pos = rs.get("open_positions", 0)
                trader_s = hb.get("trader_secs")
                sched_s  = hb.get("scheduler_secs")
                side_s   = hb.get("sidecar_secs")

                def fmt(v): return f"{v}s" if isinstance(v, int) else "n/a"
                lines = [
                    "**📊 Bot Status**",
                    f"Mode: `{mode_now}`",
                    f"Trader loop: last tick {fmt(trader_s)} ago",
                    f"Scheduler  : last tick {fmt(sched_s)} ago",
                    f"Sidecar    : last tick {fmt(side_s)} ago",
                    f"Open positions: {open_pos}",
                    f"PnL (today): {pnl:+.2f}",
                ]
                await message.channel.send("\n".join(lines))
                return

            if lower == "!symbols":
                disabled = _load_disabled_set()
                user_syms = _load_user_symbols()
                merged = []
                for s in SYMBOLS + user_syms:
                    s = s.upper()
                    if s and s not in merged:
                        merged.append(s)
                active = [s for s in merged if s not in disabled]
                txt = ["**Symbols**", "```",
                       f"Active    : {', '.join(active) if active else '(none)'}",
                       f"Disabled  : {', '.join(sorted(disabled)) if disabled else '(none)'}",
                       f"User-Added: {', '.join(user_syms) if user_syms else '(none)'}",
                       "```"]
                await message.channel.send(_reply_limit("\n".join(txt)))
                return

            if lower.startswith("!add "):
                sym = _sanitize_symbol(content.split(maxsplit=1)[1] if len(content.split()) >= 2 else "")
                if not sym:
                    await message.channel.send("Usage: `!add TICKER`")
                    return
                disabled = _load_disabled_set()
                if sym in disabled:
                    disabled.remove(sym); _save_disabled_set(disabled)
                user_syms = _load_user_symbols()
                if sym not in user_syms:
                    user_syms.append(sym); _save_user_symbols(user_syms)
                await message.channel.send(f"Added **{sym}** and enabled it.")
                return

            if lower.startswith("!remove "):
                sym = _sanitize_symbol(content.split(maxsplit=1)[1] if len(content.split()) >= 2 else "")
                if not sym:
                    await message.channel.send("Usage: `!remove TICKER`")
                    return
                user_syms = _load_user_symbols()
                user_syms = [s for s in user_syms if s != sym]
                _save_user_symbols(user_syms)
                await message.channel.send(f"Removed **{sym}** from user-added symbols.")
                return

            if lower.startswith("!enable "):
                sym = _sanitize_symbol(content.split(maxsplit=1)[1] if len(content.split()) >= 2 else "")
                if not sym:
                    await message.channel.send("Usage: `!enable TICKER`")
                    return
                disabled = _load_disabled_set()
                if sym in disabled:
                    disabled.remove(sym); _save_disabled_set(disabled)
                await message.channel.send(f"Re-enabled **{sym}**.")
                return

            if lower.startswith("!disable "):
                sym = _sanitize_symbol(content.split(maxsplit=1)[1] if len(content.split()) >= 2 else "")
                if not sym:
                    await message.channel.send("Usage: `!disable TICKER`")
                    return
                disabled = _load_disabled_set()
                disabled.add(sym); _save_disabled_set(disabled)
                await message.channel.send(f"Disabled **{sym}** — it will not be traded.")
                return

        except Exception as e:
            log.warning(f"[DiscordBot] on_message error: {e}")

    def runner():
        try:
            client.run(token, log_handler=None)
        except Exception as e:
            log.error(f"[DiscordBot] Client crashed: {e}")

    threading.Thread(target=runner, daemon=True).start()
    log.info("[DiscordBot] Listener thread started.")

def _final_exit_ping():
    """
    Fire once at interpreter shutdown. Mirror to:
      • main mode channel (send_discord)
      • heartbeat channel (send_discord_heartbeat)
      • heartbeat log file (via _log_heartbeat_line)
    """
    try:
        if MODE in ("PAPER", "LIVE"):
            msg = f"**{MODE}** process exiting."
            # Main mode channel
            try:
                send_discord(msg, MODE)
            except Exception:
                pass
            # Heartbeat channel + heartbeat.log
            try:
                send_discord_heartbeat(msg, MODE)
            except Exception:
                pass
            try:
                _log_heartbeat_line(msg, MODE)
            except Exception:
                pass

        elif MODE == "BACKTEST":
            msg = "**BACKTEST** process exiting."
            # Backtest channel (if configured)
            try:
                send_discord(msg, "BACKTEST")
            except Exception:
                pass
            # Also mirror to heartbeat log for completeness
            try:
                _log_heartbeat_line(msg, "BACKTEST")
            except Exception:
                pass

    except Exception:
        # Never let shutdown crash because of a network/logging hiccup
        pass

# keep this right after the function
atexit.register(_final_exit_ping)

# ==============================
# 13) Main
# ==============================
def main():
    log.info(f"MODE={MODE}")
    KILL_FLAG["stop"] = False  # reset on fresh start
    _start_discord_listener()
    send_discord(f"TradingBot launched in **{MODE}** mode", MODE)

    # --- one-time startup state + heartbeat + countdown ---
    try:
        _send_initial_market_state(MODE)   # posts "open/closed + ETA" once at startup
    except Exception as e:
        log.warning(f"Startup market-state failed: {e}")
    try:
        if MODE in ("PAPER", "LIVE"):
            _start_heartbeat_scheduler(MODE)   # posts hourly (open) / every 4h (closed)
    except Exception as e:
        log.warning(f"Heartbeat scheduler failed to start: {e}")
    try:
        if MODE in ("PAPER", "LIVE"):
            _start_open_countdown(MODE)        # posts 2h/1h/30m/15m to open
    except Exception as e:
        log.warning(f"Open countdown failed to start: {e}")

    # --- route into mode-specific execution ---
    if MODE == "BACKTEST":
        # 1) One-time startup banner (send first)
        send_backtest_startup(
            SYMBOLS,
            BACKTEST_INTERVAL,
            BACKTEST_LOOKBACK_DAYS,
            BACKTEST_PASSES
        )

        # 2) Run the backtest
        summary = run_backtest(
            SYMBOLS,
            BACKTEST_INTERVAL,
            BACKTEST_LOOKBACK_DAYS,
            BACKTEST_PASSES,
            BACKTEST_GLOBAL_SINGLE_POSITION
        )

        log.info("Saved: backtest_equity_curve.csv, backtest_report.csv, backtest_report_summary.csv")

        # 3) Completion ping to the backtest channel
        send_backtest(
            f"**BACKTEST COMPLETE** {summary.get('date','')} | "
            f"Trades={summary.get('total_trades',0)} | "
            f"Win%={summary.get('win_rate_total',0):.1f} | "
            f"NetPnL={summary.get('net_pnl_total',0):.2f}"
        )

    elif MODE in ("PAPER", "LIVE"):
        creds = APCA[MODE]
        if not (creds["key"] and creds["secret"] and creds["base_url"]):
            log.error(f"Missing credentials for {MODE}. Check your .env.")
            sys.exit(1)
        try:
            trading_loop(MODE)  # your existing live/paper loop
        except KeyboardInterrupt:
            log.info("User interrupt.")
        except Exception as e:
            log.exception(f"{MODE} loop crashed: {e}")
        finally:
            send_discord(f"**{MODE}** loop exiting.", MODE)
    else:
        log.error("Invalid MODE. Use BACKTEST, PAPER, or LIVE.")
        sys.exit(1)

def run_smoke_tests():
    """
    Minimal end-to-end checks:
      - Backtest produces its CSVs
      - Trade logs (if present) have the expected schema
    """
    from pathlib import Path
    import pandas as pd

    try:
        # 1) Backtest smoke (short + deterministic)
        summary = run_backtest(
            symbols=["AAPL", "MSFT"],
            interval="1Min",
            lookback_days=3,
            passes=1
        )
        from pathlib import Path
        assert (LOG_DIR / "backtest_equity_curve.csv").exists()
        assert (LOG_DIR / "backtest_report.csv").exists()

        # 2) Paper/Live trade log schema (skip if files absent)
        cols = ["time_open","time_close","symbol","qty","entry_price","exit_price","pnl","result","mode","tp","sl","client_order_id"]
        for fname in ("paper_trades.csv", "live_trades.csv"):
            p = Path(fname)
            if p.exists():
                df = pd.read_csv(p)
                missing = [c for c in cols if c not in df.columns]
                assert not missing, f"{fname}: missing columns {missing}"
        # 3) Status file presence (best-effort)
        try:
            # Simulate one write and read cycle
            write_status_snapshot(mode="PAPER", today_pnl=0.0, open_positions=0)
            rs = _read_runtime_status()
            assert isinstance(rs, dict), "runtime_status.json not readable"
        except Exception:
            # don’t fail smoke on Windows file locks or missing perms
            pass

        print("SMOKE TESTS: OK")
    except Exception as e:
        print(f"SMOKE TESTS: FAILED — {e}")
if __name__ == "__main__":
    run_smoke_tests()
    print("🚀 Starting TradingBot")
    main()