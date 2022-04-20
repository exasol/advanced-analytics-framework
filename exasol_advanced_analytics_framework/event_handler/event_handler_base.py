from abc import ABC, abstractmethod
from typing import Any, Dict

from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResult
from exasol_advanced_analytics_framework.context_wrapper.context_wrapper_base \
    import ContextWrapperBase


class EventHandlerBase(ABC):
    def __init__(self, parameters: Dict[str, Any]):
        self.parameters = parameters

    @abstractmethod
    def handle_event(
            self,
            exa_context: ContextWrapperBase,
            event_handler_context: EventHandlerContext) -> EventHandlerResult:
        raise NotImplementedError

    def cleanup(self):
        pass


