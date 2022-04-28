from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column


@dataclass()
class EventHandlerReturnQuery:
    query: str
    query_columns: List[Column]


@dataclass()
class EventHandlerResultBase:
    status: str
    query_list: List[str]


@dataclass()
class EventHandlerResultContinue(EventHandlerResultBase):
    return_query: Optional[EventHandlerReturnQuery]


@dataclass()
class EventHandlerResultFinished(EventHandlerResultBase):
    final_result: Dict[str, Any]
