from exasol_advanced_analytics_framework.event_context.event_context_base import \
    EventContextBase
from exasol_advanced_analytics_framework.event_handler.event_handler_base import \
    EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.event_handler_context import \
    EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result import \
    EventHandlerResultBase, EventHandlerResultFinished, EventHandlerReturnQuery, \
    EventHandlerResultContinue

FINAL_RESULT = {"a": "1"}
QUERY_LIST = ["SELECT 1 FROM DUAL", "SELECT 2 FROM DUAL"]


class MockEventHandlerWithOneIteration(EventHandlerBase):
    def handle_event(self,
                     exa_context: EventContextBase,
                     event_handler_context: EventHandlerContext) -> \
            EventHandlerResultBase:
        return EventHandlerResultFinished(final_result=FINAL_RESULT)


class MockEventHandlerWithTwoIterations(EventHandlerBase):
    def handle_event(self,
                     exa_context: EventContextBase,
                     event_handler_context: EventHandlerContext) -> \
            EventHandlerResultBase:

        is_last_iteration = exa_context.ctx[0] > 0
        if is_last_iteration:
            event_handler_result = EventHandlerResultFinished(
                final_result=FINAL_RESULT)
        else:

            return_query = "SELECT AAF_EVENT_HANDLER_UDF(" \
                           "1, 'bucketfs_connection', " \
                           "'MockEventHandlerWithTwoIterations')"
            event_handler_return_query = EventHandlerReturnQuery(
                query=return_query,
                query_columns=[])
            event_handler_result = EventHandlerResultContinue(
                query_list=QUERY_LIST,
                return_query=event_handler_return_query)
        return event_handler_result
