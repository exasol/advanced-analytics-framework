from dataclasses import dataclass
from typing import Optional, List


@dataclass()
class EventHandlerResult:
    return_query: Optional[str]
    status: str
    query_list: List[str]
