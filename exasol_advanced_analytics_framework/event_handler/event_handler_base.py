from abc import ABC, abstractmethod
from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResult
from exasol_advanced_analytics_framework.utils.udf_context_wrapper \
    import UDFContextWrapper


class EventHandlerBase(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def handle_event(
            self,
            udf_context: UDFContextWrapper,
            event_handler_context: EventHandlerContext) -> EventHandlerResult:
        raise NotImplementedError

    def cleanup(self):
        pass


