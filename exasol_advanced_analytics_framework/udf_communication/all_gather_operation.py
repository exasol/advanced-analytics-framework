from typing import List, Optional

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.communicator_protocol import CommunicatorProtocol
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message


class AllGatherResult(BaseModel):
    gather_result: List[bytes]


class AllGatherOperation:

    def __init__(self, communicator: CommunicatorProtocol, value: bytes):
        self._value = value
        self._communicator = communicator

    def __call__(self) -> List[bytes]:
        gather_result = self._communicator.gather(self._value)
        broadcast_value: Optional[bytes] = None
        if self._communicator.is_multi_node_leader():
            all_gather_result = AllGatherResult(gather_result=gather_result)
            broadcast_value = serialize_message(all_gather_result)
        broadcast_result = self._communicator.broadcast(broadcast_value)
        all_gather_result = deserialize_message(broadcast_result, AllGatherResult)
        return all_gather_result.gather_result
