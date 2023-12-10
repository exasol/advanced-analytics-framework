from typing import Optional, List, Protocol, runtime_checkable

import structlog
from structlog.typing import FilteringBoundLogger

LOCALHOST_LEADER_RANK = 0
MULTI_NODE_LEADER_RANK = 0

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)

@runtime_checkable
class CommunicatorProtocol(Protocol):

    def gather(self, value: bytes) -> Optional[List[bytes]]:
        pass

    def broadcast(self, value: Optional[bytes]) -> bytes:
        pass

    def all_gather(self, value: bytes) -> List[bytes]:
        pass

    def is_multi_node_leader(self):
        pass
