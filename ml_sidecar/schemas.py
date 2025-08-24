from datetime import datetime

def universe_snapshot(ts: datetime, session: str, candidates: list[str], market: dict | None = None) -> dict:
    return {"event": "universe_snapshot", "ts": ts.isoformat(), "session": session,
            "candidates": candidates, "market": market or {}}

def feature_snapshot(ts: datetime, session: str, symbol: str, features: dict[str,float],
                     feature_version: str, strategy_version: str, model_version: str | None, mode: str) -> dict:
    return {"event": "feature_snapshot", "ts": ts.isoformat(), "session": session, "symbol": symbol,
            "features": features, "feature_version": feature_version, "strategy_version": strategy_version,
            "model_version": model_version, "mode": mode}

def decision(ts: datetime, session: str, symbol: str, action: str, reason: str, params: dict | None = None) -> dict:
    return {"event": "decision", "ts": ts.isoformat(), "session": session,
            "symbol": symbol, "action": action, "reason": reason, "params": params or {}}

def execution(ts_open: str, ts_close: str, session: str, symbol: str, side: str,
              qty: float, entry_price: float, exit_price: float, pnl: float, client_order_id: str) -> dict:
    return {"event": "execution", "ts": ts_close, "session": session, "symbol": symbol,
            "side": side, "qty": qty, "entry_price": entry_price, "exit_price": exit_price,
            "pnl": pnl, "client_order_id": client_order_id}

def outcome(session: str, symbol: str, label_tp_before_sl: int | None, label_win_R: float | None,
            max_fav_excursion: float | None, max_adv_excursion: float | None, num_trades: int) -> dict:
    return {"event": "outcome", "session": session, "symbol": symbol,
            "label_tp_before_sl": label_tp_before_sl, "label_win_R": label_win_R,
            "max_fav_excursion": max_fav_excursion, "max_adv_excursion": max_adv_excursion,
            "num_trades": num_trades}
