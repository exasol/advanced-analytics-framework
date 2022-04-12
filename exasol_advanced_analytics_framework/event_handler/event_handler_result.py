from dataclasses import dataclass
from typing import Optional, List


@dataclass(unsafe_hash=True)
class EventHandlerResult:
    return_query: Optional[str]
    status: str
    query_list: List[str]
