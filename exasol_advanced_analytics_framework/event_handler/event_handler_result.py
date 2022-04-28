from dataclasses import dataclass
from typing import Optional, List
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column


@dataclass()
class EventHandlerReturnQuery:
    query: str
    query_columns: List[Column]


@dataclass()
class EventHandlerResult:
    return_query: Optional[EventHandlerReturnQuery]
    status: str
    query_list: List[str]
