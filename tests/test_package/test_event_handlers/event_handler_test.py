from typing import Dict, Any
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column
from exasol_data_science_utils_python.preprocessing.sql.schema.column_name \
    import ColumnName
from exasol_data_science_utils_python.preprocessing.sql.schema.column_type \
    import ColumnType
from exasol_advanced_analytics_framework.event_context.event_context_base \
    import EventContext
from exasol_advanced_analytics_framework.event_handler.context.scope_event_handler_context import \
    ScopeEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_base \
    import EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.context.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResultBase, EventHandlerResultFinished, \
    EventHandlerReturnQuery, EventHandlerResultContinue

FINAL_RESULT = {"result": 1}
QUERY_LIST = ["SELECT 1 FROM DUAL", "SELECT 2 FROM DUAL"]


class EventHandlerTestWithOneIteration(EventHandlerBase):
    def handle_event(self,
                     exa_context: EventContext,
                     event_handler_context: EventHandlerContext) -> \
            EventHandlerResultBase:
        return EventHandlerResultFinished(final_result=FINAL_RESULT)


class EventHandlerTestWithTwoIteration(EventHandlerBase):
    def __init__(self, parameters: Dict[str, Any]):
        super().__init__(parameters)
        self.iter = 0

    def handle_event(self,
                     exa_context: EventContext,
                     event_handler_context: EventHandlerContext) -> \
            EventHandlerResultBase:

        if self.iter > 0:
            event_handler_result = EventHandlerResultFinished(
                final_result=FINAL_RESULT)
        else:
            return_query = "SELECT 1 AS COL1, 2 AS COL2 FROM DUAL"
            return_query_columns = [
                Column(ColumnName("COL1"), ColumnType("INTEGER")),
                Column(ColumnName("COL2"), ColumnType("INTEGER"))]
            event_handler_return_query = EventHandlerReturnQuery(
                query=return_query,
                query_columns=return_query_columns)
            event_handler_result = EventHandlerResultContinue(
                query_list=QUERY_LIST,
                return_query=event_handler_return_query)
        self.iter += 1
        return event_handler_result

class EventHandlerWithOneIterationWithNotReleasedChildEventHandlerContext(EventHandlerBase):
    def handle_event(self,
                     exa_context: EventContext,
                     event_handler_context: ScopeEventHandlerContext) -> \
            EventHandlerResultBase:
        self.child = event_handler_context.get_child_event_handler_context()
        return EventHandlerResultFinished(final_result=FINAL_RESULT)

class EventHandlerWithOneIterationWithNotReleasedTemporaryObject(EventHandlerBase):
    def handle_event(self,
                     exa_context: EventContext,
                     event_handler_context: ScopeEventHandlerContext) -> \
            EventHandlerResultBase:
        self.child = event_handler_context.get_child_event_handler_context()
        self.proxy = self.child.get_temporary_table()
        return EventHandlerResultFinished(final_result=FINAL_RESULT)
