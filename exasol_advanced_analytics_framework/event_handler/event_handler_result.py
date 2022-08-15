from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column


@dataclass()
class EventHandlerReturnQuery:
    query: str
    query_columns: List[Column]


@dataclass()
class EventHandlerResultBase:
    pass


@dataclass()
class EventHandlerResultContinue(EventHandlerResultBase):
    query_list: List[str]
    return_query: Optional[EventHandlerReturnQuery]


@dataclass()
class EventHandlerResultFinished(EventHandlerResultBase):
    final_result: Dict[str, Any]
