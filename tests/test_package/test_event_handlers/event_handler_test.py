from exasol_advanced_analytics_framework.event_context.event_context_base \
    import EventContextBase
from exasol_advanced_analytics_framework.event_handler.event_handler_base \
    import EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResultBase, EventHandlerResultFinished


FINAL_RESULT = {"result": 1}


class EventHandlerTest(EventHandlerBase):
    def handle_event(self,
                     exa_context: EventContextBase,
                     event_handler_context: EventHandlerContext) -> \
            EventHandlerResultBase:
        return EventHandlerResultFinished(final_result=FINAL_RESULT)

