from abc import ABC, abstractmethod
from typing import Any, Dict

from exasol_advanced_analytics_framework.event_context.event_context_base import EventContext
from exasol_advanced_analytics_framework.event_handler.context.scope_event_handler_context import \
    ScopeEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResultBase


class EventHandlerBase(ABC):
    def __init__(self, parameters: Dict[str, Any]):
        self.parameters = parameters

    @abstractmethod
    def handle_event(
            self,
            exa_context: EventContext,
            event_handler_context: ScopeEventHandlerContext) \
            -> EventHandlerResultBase:
        raise NotImplementedError

    def cleanup(self):
        pass
