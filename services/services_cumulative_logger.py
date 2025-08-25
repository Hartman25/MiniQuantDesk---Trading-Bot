from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone

import pandas as pd


def ensure_dir(p: Path) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def append_row(row: Dict[str, Any], out_path: Path) -> None:
    """Append a single row to CSV, creating header only once."""
    out_path = Path(out_path)
    ensure_dir(out_path.parent)
    df = pd.DataFrame([row])
    write_header = not out_path.exists() or out_path.stat().st_size == 0
    df.to_csv(out_path, mode="a", header=write_header, index=False)
    try:
        logging.info(f"[APPEND] 1 row -> {out_path.resolve()}")
    except Exception:
        pass


def build_pass_row(mode: str, interval: str, i: int, symbols, equity: Optional[float]) -> Dict[str, Any]:
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "interval": interval,
        "pass": i,
        "symbols": ",".join(list(symbols)) if isinstance(symbols, (list, tuple)) else str(symbols),
        "equity": float(equity) if equity is not None else None,
    }


def build_summary_row(summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mode": summary.get("mode"),
        "interval": summary.get("interval"),
        "passes": summary.get("passes"),
        "symbols": ",".join(summary.get("symbols", [])),
        "total_trades": summary.get("total_trades"),
        "total_wins": summary.get("total_wins"),
        "win_rate_total": summary.get("win_rate_total"),
        "net_pnl_total": summary.get("net_pnl_total"),
    }
