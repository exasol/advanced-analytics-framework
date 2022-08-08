from typing import Dict, Any

from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column
from exasol_data_science_utils_python.preprocessing.sql.schema.column_name import \
    ColumnName
from exasol_data_science_utils_python.preprocessing.sql.schema.column_type import \
    ColumnType

from exasol_advanced_analytics_framework.event_context.event_context_base \
    import EventContextBase
from exasol_advanced_analytics_framework.event_handler.event_handler_base \
    import EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.context.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResultBase, EventHandlerResultFinished, \
    EventHandlerReturnQuery, EventHandlerResultContinue


FINAL_RESULT = {"a": "1"}
QUERY_LIST = ["SELECT 1 FROM DUAL", "SELECT 2 FROM DUAL"]


class MockEventHandlerWithOneIteration(EventHandlerBase):
    def handle_event(self,
                     exa_context: EventContextBase,
                     event_handler_context: EventHandlerContext) -> \
            EventHandlerResultBase:
        return EventHandlerResultFinished(final_result=FINAL_RESULT)


class MockEventHandlerWithTwoIterations(EventHandlerBase):
    def __init__(self, parameters: Dict[str, Any]):
        super().__init__(parameters)
        self.iter = 0

    def handle_event(self,
                     exa_context: EventContextBase,
                     event_handler_context: EventHandlerContext) -> \
            EventHandlerResultBase:

        if self.iter > 0:
            event_handler_result = EventHandlerResultFinished(
                final_result=FINAL_RESULT)
        else:
            return_query = "SELECT a, table1.b, c FROM table1, table2 " \
                           "WHERE table1.b=table2.b"
            return_query_columns = [
                Column(ColumnName("a"), ColumnType("INTEGER")),
                Column(ColumnName("b"), ColumnType("INTEGER"))]
            event_handler_return_query = EventHandlerReturnQuery(
                query=return_query,
                query_columns=return_query_columns)
            event_handler_result = EventHandlerResultContinue(
                query_list=QUERY_LIST,
                return_query=event_handler_return_query)
        self.iter += 1
        return event_handler_result
