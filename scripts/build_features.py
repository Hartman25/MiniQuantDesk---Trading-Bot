
#!/usr/bin/env python3
"""
build_features.py — compute simple features/labels from partitioned market data

Reads:
  ml_sidecar/data/ml/market/<SYMBOL>/<INTERVAL>/YYYY/MM/DD.parquet
Writes:
  ml_sidecar/data/ml/features/<SYMBOL>/<INTERVAL>/YYYY/MM/DD.parquet

Features:
  - SMA_F, SMA_S (from env SMA_FAST/SMA_SLOW or defaults 5/20)
  - RSI (from env RSI_PERIOD or default 14)

Label:
  - target_up: 1 if next Close > current Close, else 0

Usage:
  python scripts/build_features.py --symbols AAPL,MSFT --interval 1Day
"""

import os
import sys
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("DATA_DIR", ROOT / "ml_sidecar" / "data" / "ml"))

SMA_FAST = int(os.getenv("SMA_FAST", "5"))
SMA_SLOW = int(os.getenv("SMA_SLOW", "20"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))

def _infer_interval(env_interval: str | None) -> str:
    s = (env_interval or "").lower()
    if s in ("1min", "1m", "minute"):
        return "1Min"
    return "1Day"

def _parse_symbols(env_symbols: str | None, override: str | None) -> list[str]:
    src = (override or env_symbols or "AAPL,MSFT")
    return [s.strip().upper() for s in src.split(",") if s.strip()]

def _load_symbol_frames(symbol: str, interval: str) -> pd.DataFrame:
    base = DATA_ROOT / "market" / symbol / interval
    if not base.exists():
        return pd.DataFrame()
    # concatenate daily partitions
    parts = sorted(base.rglob("*.parquet"))
    if not parts:
        return pd.DataFrame()
    frames = []
    for p in parts:
        try:
            df = pd.read_parquet(p)
        except Exception:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    # ensure dtypes and sort by time
    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    df = df.dropna(subset=["Time"]).sort_values("Time")
    return df

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out

def _write_partitions(df: pd.DataFrame, symbol: str, interval: str) -> int:
    if df.empty:
        return 0
    # partition by day (based on Time)
    df = df.copy()
    df["date"] = df["Time"].dt.date
    count = 0
    for day, grp in df.groupby("date", sort=True):
        out_path = DATA_ROOT / "features" / symbol / interval / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        grp.drop(columns=["date"], inplace=True)
        grp.to_parquet(out_path, engine="pyarrow", index=False)
        count += len(grp)
    return count

def build_for_symbol(symbol: str, interval: str) -> int:
    raw = _load_symbol_frames(symbol, interval)
    if raw.empty:
        print(f"[WARN] no market data for {symbol} {interval}")
        return 0

    d = raw.copy()
    d["SMA_F"] = sma(d["Close"], SMA_FAST)
    d["SMA_S"] = sma(d["Close"], SMA_SLOW)
    d["RSI"]   = rsi(d["Close"], RSI_PERIOD)
    # simple forward label
    d["Close_next"] = d["Close"].shift(-1)
    d["target_up"] = (d["Close_next"] > d["Close"]).astype("int8")
    d = d.dropna().reset_index(drop=True)

    wrote = _write_partitions(d, symbol, interval)
    return wrote

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=str, default=None, help="Comma-separated symbols (override env SYMBOLS)")
    ap.add_argument("--interval", type=str, default=None, help="1Day or 1Min (override env BACKTEST_INTERVAL)")
    args = ap.parse_args()

    env_symbols = os.getenv("SYMBOLS", "AAPL,MSFT")
    env_interval = os.getenv("BACKTEST_INTERVAL", "1Day")
    interval = _infer_interval(args.interval or env_interval)
    symbols = _parse_symbols(env_symbols, args.symbols)

    print(f"[INFO] Building features interval={interval}, symbols={symbols}")
    total = 0
    for sym in symbols:
        wrote = build_for_symbol(sym, interval)
        print(f"[DONE] {sym}: wrote {wrote} rows of features")
        total += wrote
    print(f"[SUMMARY] total feature rows written: {total}")

if __name__ == "__main__":
    main()
