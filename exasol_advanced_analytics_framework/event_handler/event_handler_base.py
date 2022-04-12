from abc import ABC, abstractmethod
from exasol_advanced_analytics_framework.event_handler.event_handler_result import \
    EventHandlerResult


class EventHandlerBase(ABC):
    def __init__(self):
        self.state = None

    @abstractmethod
    def handle_event(self) -> EventHandlerResult:
        raise NotImplementedError



    def cleanup(self):
        pass


