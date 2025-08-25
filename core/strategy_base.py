
# core/strategy_base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any
import os

@dataclass
class StrategyContext:
    """Holds shared params read from env (or elsewhere)."""
    take_profit_pct: float = float(os.getenv("TAKE_PROFIT_PCT", "0.05"))
    stop_loss_pct: float   = float(os.getenv("STOP_LOSS_PCT", "0.02"))
    risk_per_trade: float  = float(os.getenv("RISK_PER_TRADE", "0.01"))
    sma_fast: int          = int(os.getenv("SMA_FAST", "5"))
    sma_slow: int          = int(os.getenv("SMA_SLOW", "20"))
    rsi_period: int        = int(os.getenv("RSI_PERIOD", "14"))
    rsi_min_buy: float     = float(os.getenv("RSI_MIN_BUY", "45"))
    rsi_max_sell: float    = float(os.getenv("RSI_MAX_SELL", "70"))

class Strategy(ABC):
    """Abstract base for strategies. Concrete strategies must implement two methods."""

    def __init__(self, name: str, ctx: StrategyContext | None = None) -> None:
        self.name = name
        self.ctx = ctx or StrategyContext()

    @abstractmethod
    def warmup_bars(self) -> int:
        """How many bars are needed before signals are valid (e.g., due to SMAs/RSI)."""
        raise NotImplementedError

    @abstractmethod
    def generate_signals(self, df, symbol: str):
        """
        Given a price DataFrame with at least ['Time','Open','High','Low','Close','Volume'],
        return a copy with boolean columns 'CROSS_UP' and 'CROSS_DOWN' (and any extras).
        """
        raise NotImplementedError
