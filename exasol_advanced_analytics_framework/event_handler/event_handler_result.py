from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass()
class EventHandlerResult:
    return_query: Optional[str]
    return_query_columns: Optional[Dict[str, Any]]
    status: str
    query_list: List[str]
