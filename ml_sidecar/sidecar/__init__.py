# ml_sidecar/sidecar/__init__.py

# Ensure event_bus submodule is importable via `from ml_sidecar.sidecar import event_bus`
from . import event_bus

# Expose key symbols at the sidecar package level
from .event_bus import EventBus, bus

__all__ = ["event_bus", "EventBus", "bus"]
