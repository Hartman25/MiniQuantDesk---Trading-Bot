
#!/usr/bin/env python3
"""
fetch_bars.py — pull historical OHLCV and write partitioned Parquet

Sources: Yahoo Finance (via yfinance)
- Daily: up to many years
- 1-minute: Yahoo typically allows ~30 days; we chunk by day

Output layout:
ml_sidecar/data/ml/market/<SYMBOL>/<INTERVAL>/YYYY/MM/DD.parquet

Usage:
  python scripts/fetch_bars.py                      # uses env SYMBOLS + BACKTEST_INTERVAL, auto lookback
  python scripts/fetch_bars.py --symbols AAPL,MSFT  # override symbols
  python scripts/fetch_bars.py --interval 1Min      # override interval
  python scripts/fetch_bars.py --lookback 5y        # override lookback (e.g., 5y, 365d, 14d)
  python scripts/fetch_bars.py --force              # re-download and overwrite existing partitions

Requires: yfinance, pandas, numpy, pyarrow
"""

import os
import sys
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import numpy as np

# Lazy import yfinance so script can be listed without failing
try:
    import yfinance as yf
except Exception as e:
    print("[ERR] yfinance not installed. pip install yfinance", file=sys.stderr)
    raise

ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("DATA_DIR", ROOT / "ml_sidecar" / "data" / "ml"))

def _infer_interval(env_interval: str | None) -> str:
    s = (env_interval or "").lower()
    if s in ("1min", "1m", "minute"):
        return "1Min"
    return "1Day"

def _default_lookback(interval: str) -> str:
    # Heuristic: daily -> 5y ; minute -> 30d (Yahoo constraint)
    return "5y" if interval.lower().startswith("1d") else "30d"

def _parse_symbols(env_symbols: str | None, override: str | None) -> list[str]:
    src = (override or env_symbols or "AAPL,MSFT")
    return [s.strip().upper() for s in src.split(",") if s.strip()]

def _parse_lookback(arg: str | None, interval: str) -> timedelta:
    txt = (arg or _default_lookback(interval)).strip().lower()
    if txt.endswith("y"):
        years = int(txt[:-1])
        return timedelta(days=365*years)
    if txt.endswith("d"):
        days = int(txt[:-1])
        return timedelta(days=days)
    # default days
    return timedelta(days=int(txt))

def _iter_partitions(start_dt: datetime, end_dt: datetime, interval: str):
    """
    Yield (p_start, p_end) UTC windows to download in manageable chunks.
    - For 1Day: chunk by month
    - For 1Min: chunk by day
    """
    tz_utc = timezone.utc
    cur = start_dt.replace(tzinfo=tz_utc)
    end = end_dt.replace(tzinfo=tz_utc)
    if interval.lower().startswith("1d"):
        # monthly chunks
        while cur < end:
            year = cur.year
            month = cur.month
            # next month
            if month == 12:
                nxt = datetime(year+1, 1, 1, tzinfo=tz_utc)
            else:
                nxt = datetime(year, month+1, 1, tzinfo=tz_utc)
            yield (cur, min(nxt, end))
            cur = nxt
    else:
        # minute -> daily chunks
        while cur < end:
            nxt = (cur + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            yield (cur, min(nxt, end))
            cur = nxt

def _yf_interval(interval: str) -> str:
    return "1m" if interval.lower().startswith("1d") is False else "1d"

def _write_parquet(df: pd.DataFrame, symbol: str, interval: str):
    if df.empty:
        return 0
    # Normalize columns
    out = df.rename(columns={"Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume"}).copy()
    out = out[["Open","High","Low","Close","Volume"]]
    out.insert(0, "Time", df.index.tz_convert("UTC").tz_localize(None) if df.index.tz is not None else df.index)  # naive UTC
    # Partition path by first timestamp's date
    count = 0
    for ts, row in out.iterrows():
        day = pd.Timestamp(ts).to_pydatetime().date()
        p = DATA_ROOT / "market" / symbol / interval / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        # Append-friendly: single-row parquet files; simpler to resume/merge later
        row_df = pd.DataFrame([row.to_dict()])
        row_df.to_parquet(p, engine="pyarrow", index=False, append=False)  # overwrite per day file per run
        count += 1
    return count

def fetch_symbol(symbol: str, interval: str, lookback_td, force: bool=False) -> int:
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - lookback_td
    yf_int = _yf_interval(interval)

    wrote = 0
    for p_start, p_end in _iter_partitions(start_dt, end_dt, interval):
        # Skip if all target day files already exist and not forcing
        if not force and interval.lower().startswith("1d"):
            # check month existence: if the first day parquet exists, assume done
            probe = DATA_ROOT / "market" / symbol / interval / f"{p_start:%Y}" / f"{p_start:%m}"
            if probe.exists() and any(probe.glob("*.parquet")):
                continue
        try:
            df = yf.download(
                tickers=symbol,
                interval=yf_int,
                start=p_start,
                end=p_end,
                auto_adjust=False,
                progress=False,
                threads=True,
            )
        except Exception as e:
            print(f"[WARN] yfinance download failed for {symbol} {p_start}..{p_end}: {e}", file=sys.stderr)
            continue

        if df is None or df.empty:
            continue
        wrote += _write_parquet(df, symbol, interval)
    return wrote

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=str, default=None, help="Comma-separated symbols (override env SYMBOLS)")
    ap.add_argument("--interval", type=str, default=None, help="1Day or 1Min (override env BACKTEST_INTERVAL)")
    ap.add_argument("--lookback", type=str, default=None, help="e.g., 5y, 365d, 30d (default 5y for 1Day, 30d for 1Min)")
    ap.add_argument("--force", action="store_true", help="Force re-download and overwrite existing partitions")
    args = ap.parse_args()

    env_symbols = os.getenv("SYMBOLS", "AAPL,MSFT")
    env_interval = os.getenv("BACKTEST_INTERVAL", "1Day")

    interval = _infer_interval(args.interval or env_interval)
    symbols = _parse_symbols(env_symbols, args.symbols)
    lookback_td = _parse_lookback(args.lookback, interval)

    print(f"[INFO] Fetching interval={interval}, lookback={lookback_td}, symbols={symbols}")
    total = 0
    for sym in symbols:
        wrote = fetch_symbol(sym, interval, lookback_td, force=args.force)
        print(f"[DONE] {sym}: wrote {wrote} rows (parquet partitions)")
        total += wrote
    print(f"[SUMMARY] total rows written: {total}")

if __name__ == "__main__":
    main()
