
# strategies/abcd.py
from __future__ import annotations
import pandas as pd
import numpy as np
from core.strategy_base import Strategy, StrategyContext

def _sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)

class StrategyABCD(Strategy):
    """Baseline ABCD-ish momentum strategy using SMA cross with RSI filter."""

    def __init__(self, ctx: StrategyContext | None = None) -> None:
        super().__init__(name="ABCD", ctx=ctx)

    def warmup_bars(self) -> int:
        return max(int(self.ctx.sma_slow), int(self.ctx.rsi_period)) + 2

    def generate_signals(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        d = df.copy()
        d["SMA_F"] = _sma(d["Close"], int(self.ctx.sma_fast))
        d["SMA_S"] = _sma(d["Close"], int(self.ctx.sma_slow))
        d["RSI"]   = _rsi(d["Close"], int(self.ctx.rsi_period))

        # Entry when fast crosses above slow and RSI confirms
        d["CROSS_UP"] = (
            (d["SMA_F"].shift(1) <= d["SMA_S"].shift(1)) &
            (d["SMA_F"] > d["SMA_S"]) &
            (d["RSI"] >= float(self.ctx.rsi_min_buy))
        )

        # Exit when fast crosses below slow or RSI is hot
        d["CROSS_DOWN"] = (
            (d["SMA_F"].shift(1) >= d["SMA_S"].shift(1)) &
            (d["SMA_F"] < d["SMA_S"])
        ) | (d["RSI"] >= float(self.ctx.rsi_max_sell))

        return d
