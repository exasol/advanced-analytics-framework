from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass()
class EventHandlerReturnQuery:
    query: str
    query_columns: List[str]


@dataclass()
class EventHandlerResult:
    return_query: Optional[EventHandlerReturnQuery]
    status: str
    query_list: List[str]
