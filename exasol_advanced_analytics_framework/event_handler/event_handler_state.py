from dataclasses import dataclass
from exasol_advanced_analytics_framework.event_handler.event_handler_base \
    import EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext


@dataclass()
class EventHandlerState:
    context: EventHandlerContext
    event_handler: EventHandlerBase
