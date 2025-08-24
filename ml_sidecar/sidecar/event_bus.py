import os
import time
import zmq
import datetime as dt
from pathlib import Path

# This file lives in ml_sidecar/sidecar/, so ROOT is the ml_sidecar folder.
ROOT = Path(__file__).resolve().parents[1]

# Use top-level ml_sidecar/logs by default (overridable via env)
LOG_DIR = Path(os.getenv("LOG_DIR", str(ROOT / "logs")))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Heartbeat file (checked by preflight)
_BEAT_PATH = LOG_DIR / "beat_sidecar.txt"

# PUB bind address (supports legacy LOG_BIND env as a fallback)
BUS_BIND = os.getenv("BUS_BIND", os.getenv("LOG_BIND", "tcp://127.0.0.1:5557"))

def _beat_sidecar():
    """Touch/update the sidecar heartbeat file (UTC ISO8601)."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        _BEAT_PATH.write_text(dt.datetime.now(dt.timezone.utc).isoformat(), encoding="utf-8")
    except Exception:
        # never crash on heartbeat write
        pass

class EventBus:
    """
    ZeroMQ PUB socket for ML/telemetry events.
    Consumers (like logger_service.py) SUB connect to BUS_BIND.
    """
    def __init__(self, bind: str | None = None):
        self.addr = bind or BUS_BIND
        self.ctx = zmq.Context.instance()
        self.sock = self.ctx.socket(zmq.PUB)
        self.sock.bind(self.addr)
        # Small warm-up to avoid first-send drop on slow subscribers
        time.sleep(0.05)

    def send(self, payload: dict):
        """Publish JSON payload; never crash if send fails."""
        try:
            _beat_sidecar()
            self.sock.send_json(payload)
        except Exception:
            # fire-and-forget
            pass

# Global convenience bus
bus = EventBus()
