
"""
TradingBot.py — Pylance-clean backtest executor with modular strategies.
- Uses core.strategy_loader.load_strategy() (set STRATEGY=abcd).
- Consolidated CSV appends via services.runs_schema.
- Alpaca imports are optional; type hints use forward references to satisfy Pylance.
"""

import os, sys, json, logging, requests, datetime as dt, time
from pathlib import Path
from typing import List, Optional, Dict, TYPE_CHECKING

import numpy as np
import pandas as pd

# --- Ensure repo root is importable (so `core.*`, `services.*`, `strategies.*` resolve) ---
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Schema helper
from services.runs_schema import new_run_id, append_pass_row, append_summary_row
# Strategy loader
from core.strategy_loader import load_strategy

# Optional dotenv
try:
    from dotenv import load_dotenv as _load_dotenv
except Exception:
    _load_dotenv = None

if _load_dotenv:
    for p in (ROOT / ".env", ROOT / "env"):
        if p.exists():
            try:
                _load_dotenv(str(p), override=True)
            except Exception:
                pass

# ---------- Logging ----------
LOG_DIR = Path(os.getenv("LOG_DIR", str(ROOT / "ml_sidecar" / "logs")))
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("TradingBot")

# ---------- Discord (optional webhooks) ----------
WEBHOOKS = {
    "BACKTEST": os.getenv("DISCORD_WEBHOOK_BACKTEST", "").strip(),
    "PAPER":    os.getenv("DISCORD_WEBHOOK_PAPER", "").strip(),
    "LIVE":     os.getenv("DISCORD_WEBHOOK_LIVE", "").strip(),
}
def send_discord(msg: str, mode: Optional[str] = None) -> None:
    mode = (mode or os.getenv("MODE", "BACKTEST")).upper()
    url = WEBHOOKS.get(mode, "")
    if not url:
        return
    try:
        requests.post(url, json={"content": msg[:1900]}, timeout=5)
    except Exception as e:
        log.warning(f"[Discord] send failed: {e}")

# ---------- Config ----------
MODE = os.getenv("MODE", "BACKTEST").upper()
APPEND_ONLY = os.getenv("BACKTEST_APPEND_ONLY", "true").lower() == "true"

BACKTEST_INTERVAL        = os.getenv("BACKTEST_INTERVAL", "1Day")  # 1Min or 1Day
BACKTEST_LOOKBACK_DAYS   = int(os.getenv("BACKTEST_LOOKBACK_DAYS", "120"))
BACKTEST_PASSES          = int(os.getenv("BACKTEST_PASSES", "3"))
BACKTEST_TARGET_TRADES_PER_SYMBOL = int(os.getenv("BACKTEST_TARGET_TRADES_PER_SYMBOL", "100"))
BACKTEST_MAX_PASSES      = int(os.getenv("BACKTEST_MAX_PASSES", "50"))
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "AAPL,MSFT").split(",") if s.strip()]

TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.05"))
STOP_LOSS_PCT   = float(os.getenv("STOP_LOSS_PCT", "0.02"))
RISK_PER_TRADE  = float(os.getenv("RISK_PER_TRADE", "0.01"))

# ---------- Alpaca data (v3) for historical bars (guarded) ----------
try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from alpaca.data.enums import DataFeed
except Exception:
    StockHistoricalDataClient = None  # type: ignore[assignment]
    StockBarsRequest = None           # type: ignore[assignment]
    TimeFrame = None                  # type: ignore[assignment]
    TimeFrameUnit = None              # type: ignore[assignment]
    DataFeed = None                   # type: ignore[assignment]

# Make type-checkers happy even if runtime imports fail
if TYPE_CHECKING:
    from alpaca.data.historical import StockHistoricalDataClient as _StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest as _StockBarsRequest
    from alpaca.data.timeframe import TimeFrame as _TimeFrame, TimeFrameUnit as _TimeFrameUnit
    from alpaca.data.enums import DataFeed as _DataFeed

def build_data_client() -> "StockHistoricalDataClient | None":
    """Construct an Alpaca historical data client, or None if creds missing/not installed."""
    key = os.getenv("APCA_API_KEY_ID_PAPER") or os.getenv("APCA_API_KEY_ID")
    sec = os.getenv("APCA_API_SECRET_KEY_PAPER") or os.getenv("APCA_API_SECRET_KEY")
    if StockHistoricalDataClient is None or not key or not sec:
        return None
    try:
        return StockHistoricalDataClient(key, sec)
    except Exception as e:
        log.warning(f"[ALPACA] Failed to create data client: {e}")
        return None

def _to_timeframe(interval: str) -> "TimeFrame | None":
    if TimeFrame is None or TimeFrameUnit is None:
        return None
    s = str(interval).lower()
    if s in ("1min", "1m", "minute"):
        return TimeFrame(1, TimeFrameUnit.Minute)
    if s in ("1day", "1d", "day", "daily"):
        return TimeFrame(1, TimeFrameUnit.Day)
    return None

def build_request(symbol: str, interval: str, lookback_days: int) -> "StockBarsRequest | None":
    if StockBarsRequest is None:
        return None
    end = dt.datetime.utcnow()
    start = end - dt.timedelta(days=int(lookback_days))
    tf = _to_timeframe(interval) or _to_timeframe("1Day")
    try:
        return StockBarsRequest(
            symbol_or_symbols=symbol,
            start=start,
            end=end,
            timeframe=tf,
            feed=DataFeed.IEX if DataFeed is not None else None,
            adjustment="raw",
            limit=10_000,
        )
    except Exception as e:
        log.warning(f"[ALPACA] Failed to build request: {e}")
        return None

def fetch_bars(client: "StockHistoricalDataClient", req: "StockBarsRequest") -> pd.DataFrame:
    """Fetch bars from Alpaca v3; return a normalized DataFrame or empty df on failure."""
    try:
        resp = client.get_stock_bars(req)
        if resp is None or resp.df is None or resp.df.empty:
            return pd.DataFrame()
        df = resp.df.copy()
        # If multi-index (symbol, time), drop symbol level
        if isinstance(df.index, pd.MultiIndex) and "symbol" in df.index.names:
            df = df.reset_index(level=0, drop=True)
        df = df.reset_index().rename(columns={
            "timestamp": "Time", "open": "Open", "high": "High",
            "low": "Low", "close": "Close", "volume": "Volume"
        })
        return df[["Time", "Open", "High", "Low", "Close", "Volume"]]
    except Exception as e:
        log.warning(f"[ALPACA] fetch_bars failed: {e}")
        return pd.DataFrame()

# ---------- Helpers ----------
def qty_from_risk(equity: float, price: float, stop_loss_pct: float, risk_per_trade: float) -> int:
    """Basic position sizing based on account equity and stop distance."""
    try:
        sl_price = price * (1 - max(0.0, float(stop_loss_pct)))
        risk_per_share = max(price - sl_price, 1e-6)
        dollars_at_risk = max(0.0, float(equity)) * max(0.0, float(risk_per_trade))
        qty = int(max(dollars_at_risk / risk_per_share, 1))
        return qty
    except Exception:
        return 1

def today_str() -> str:
    return dt.datetime.utcnow().date().isoformat()

# ---------- Core backtest ----------
def run_backtest(symbols: List[str], interval: str, lookback_days: int, passes: int) -> Dict:
    log.info(f"Running BACKTEST: interval={interval}, lookback_days={lookback_days}, passes={passes}, symbols={symbols}")
    strategy, strat_ctx = load_strategy()
    run_id = new_run_id()
    send_discord(f"**BACKTEST** start | {interval} | {lookback_days}d | passes={passes} | {symbols}", "BACKTEST")

    client = build_data_client()
    if client is None:
        log.warning("[ALPACA] Data client unavailable; will simulate data for demo")

    # Backtest state
    total_trades = 0
    total_wins = 0
    net_pnl_total = 0.0

    max_allowed = min(int(passes), int(os.getenv("BACKTEST_MAX_PASSES", "50")))

    for attempt in range(1, max_allowed + 1):
        log.info(f"[Backtest] Pass {attempt}/{max_allowed}")

        # Append per-pass row (equity unknown here -> None)
        try:
            append_pass_row(
                run_id=run_id, mode="BACKTEST", interval=interval,
                pass_num=attempt, symbols=symbols, equity=None,
                out_path=LOG_DIR / "backtest_runs.csv",
            )
        except Exception as e:
            log.warning(f"[APPEND] pass row failed: {e}")

        # Iterate each symbol
        for symbol in symbols:
            # Acquire data
            if client is not None and StockBarsRequest is not None:
                req = build_request(symbol, interval, lookback_days)
                raw = fetch_bars(client, req) if req is not None else pd.DataFrame()
            else:
                # Simulated data if Alpaca not available
                n = max(150, lookback_days)  # ensure enough bars for indicators
                rng = np.random.default_rng(seed=42 + attempt)
                prices = np.cumsum(rng.normal(0, 1, size=n)) + 100.0
                freq = "D" if interval.lower().startswith("1d") else "T"
                raw = pd.DataFrame({
                    "Time": pd.date_range(end=dt.datetime.utcnow(), periods=n, freq=freq),
                    "Open": prices,
                    "High": prices + rng.random(n),
                    "Low":  prices - rng.random(n),
                    "Close": prices + rng.normal(0, 0.5, size=n),
                    "Volume": rng.integers(1000, 10000, size=n),
                })

            # Warmup check
            min_bars = max(strategy.warmup_bars(), 5)
            if raw.empty or len(raw) < min_bars:
                log.info(f"{symbol}: not enough data (need {min_bars} bars).")
                continue

            # Generate signals
            df = strategy.generate_signals(raw, symbol)

            # Very simple simulation: buy at CROSS_UP close, exit at CROSS_DOWN or TP/SL
            position = None
            entry_price = 0.0
            for i in range(min_bars, len(df)):
                row = df.iloc[i]
                price = float(row["Close"])

                if position is None and bool(row.get("CROSS_UP", False)):
                    qty = qty_from_risk(100_000.0, price, STOP_LOSS_PCT, RISK_PER_TRADE)
                    position = {"qty": qty, "entry": price}
                    entry_price = price
                    continue

                if position is not None:
                    tp = entry_price * (1 + TAKE_PROFIT_PCT)
                    sl = entry_price * (1 - STOP_LOSS_PCT)
                    exit_signal = bool(row.get("CROSS_DOWN", False)) or price >= tp or price <= sl
                    if exit_signal:
                        pnl = (price - entry_price) * position["qty"]
                        total_trades += 1
                        if pnl > 0:
                            total_wins += 1
                        net_pnl_total += pnl
                        position = None

        # Optional early stop if target trades reached
        if total_trades >= BACKTEST_TARGET_TRADES_PER_SYMBOL * max(1, len(symbols)):
            log.info("BACKTEST: all symbols reached target trades; stopping early.")
            break

    winrate_tot = (total_wins / total_trades * 100.0) if total_trades else 0.0
    summary = {
        "mode": "BACKTEST",
        "date": today_str(),
        "interval": interval,
        "symbols": symbols,
        "passes": passes,
        "total_trades": total_trades,
        "total_wins": total_wins,
        "win_rate_total": round(winrate_tot, 2),
        "net_pnl_total": round(net_pnl_total, 2),
    }
    log.info("Backtest Summary:\n" + json.dumps(summary, indent=2))

    # Append summary row
    try:
        append_summary_row(
            run_id=run_id, mode="BACKTEST", interval=summary["interval"], symbols=summary["symbols"],
            total_trades=summary["total_trades"], total_wins=summary["total_wins"],
            win_rate_total=summary["win_rate_total"], net_pnl_total=summary["net_pnl_total"],
            out_path=LOG_DIR / "backtest_runs.csv",
        )
    except Exception as e:
        log.warning(f"[APPEND] summary row failed: {e}")

    send_discord(
        f"**BACKTEST DONE** {today_str()} | Trades={total_trades} | Win%={winrate_tot:.1f} | NetPnL={net_pnl_total:.2f}",
        "BACKTEST",
    )
    return summary

if __name__ == "__main__":
    if MODE == "BACKTEST":
        res = run_backtest(SYMBOLS, BACKTEST_INTERVAL, BACKTEST_LOOKBACK_DAYS, BACKTEST_PASSES)
        log.info(f"[DONE] Backtest finished: {res}")
    else:
        log.info(f"MODE={MODE} — PAPER/LIVE not yet implemented here")
