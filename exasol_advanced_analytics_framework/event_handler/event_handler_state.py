from dataclasses import dataclass
from typing import List
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column

from exasol_advanced_analytics_framework.event_handler.context.top_level_event_handler_context import \
    TopLevelEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_base \
    import EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.context.event_handler_context \
    import EventHandlerContext


@dataclass()
class EventHandlerState:
    context: TopLevelEventHandlerContext
    event_handler: EventHandlerBase
    query_columns: List[Column]
