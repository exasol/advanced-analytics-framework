from abc import ABC, abstractmethod
from typing import Any, Dict

from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResultBase
from exasol_advanced_analytics_framework.event_context.event_context_base \
    import EventContextBase


class EventHandlerBase(ABC):
    def __init__(self, parameters: Dict[str, Any]):
        self.parameters = parameters

    @abstractmethod
    def handle_event(
            self,
            exa_context: EventContextBase,
            event_handler_context: EventHandlerContext) \
            -> EventHandlerResultBase:
        raise NotImplementedError

    def cleanup(self):
        pass


