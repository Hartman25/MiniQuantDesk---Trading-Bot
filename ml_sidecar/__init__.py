# Re-export sidecar modules for compatibility
from .sidecar import event_bus
from .sidecar.event_bus import EventBus, bus

__all__ = ["event_bus", "EventBus", "bus"]
