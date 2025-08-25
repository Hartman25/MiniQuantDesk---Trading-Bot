
# core/strategy_loader.py
from __future__ import annotations
import importlib
import os
from typing import Tuple
from core.strategy_base import Strategy, StrategyContext

DEFAULT_STRATEGY = "abcd"  # strategies/abcd.py -> class StrategyABCD

def load_strategy() -> Tuple[Strategy, StrategyContext]:
    name = os.getenv("STRATEGY", DEFAULT_STRATEGY).strip().lower()
    ctx = StrategyContext()
    if name == "abcd":
        mod = importlib.import_module("strategies.abcd")
        return mod.StrategyABCD(ctx=ctx), ctx
    # Extend here for additional strategies:
    # elif name == "rsi_breakout": ...

    # Fallback to default
    mod = importlib.import_module("strategies.abcd")
    return mod.StrategyABCD(ctx=ctx), ctx
