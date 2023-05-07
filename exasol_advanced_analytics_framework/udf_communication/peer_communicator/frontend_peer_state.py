from collections import deque
from typing import List, Deque

import structlog
from sortedcontainers import SortedSet
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import PayloadMessage, AcknowledgePayloadMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_listener_interface import \
    BackgroundListenerInterface
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory \
    import Frame

LOGGER: FilteringBoundLogger = structlog.getLogger()


class IntSet:
    def __init__(self):
        self._set: SortedSet = SortedSet()
        self._max_complete = None

    def add(self, item: int):
        if self._max_complete is None:
            self._max_complete = item
        elif item == self._max_complete + 1:
            self._max_complete = item
            self._compact_set()
        if item > self._max_complete:
            self._set.add(item)

    def __contains__(self, item: int) -> bool:
        return self._max_complete is not None and item <= self._max_complete or item in self._set

    def _compact_set(self):
        position_of_max = None
        for index, item in enumerate(self._set):
            if item == self._max_complete + 1:
                self._max_complete = item
                position_of_max = index
            else:
                break
        if position_of_max is not None:
            del self._set[0:position_of_max]


class FrontendPeerState:

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 background_listener: BackgroundListenerInterface,
                 peer: Peer):
        self._background_listener = background_listener
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._connection_is_ready = False
        self._peer_register_forwarder_is_ready = False
        self._next_send_payload_sequence_number = 0
        self._next_received_payload_sequence_number = 0
        self._received_messages: Deque[List[Frame]] = deque()
        self._received_acknowledgments: IntSet = IntSet()
        self._logger = LOGGER.bind(peer=peer.dict(), my_connection_info=my_connection_info.dict())

    def received_connection_is_ready(self):
        self._connection_is_ready = True

    def received_peer_register_forwarder_is_ready(self):
        self._peer_register_forwarder_is_ready = True

    @property
    def peer_is_ready(self) -> bool:
        return self._connection_is_ready and self._peer_register_forwarder_is_ready

    def send(self, payload: List[Frame]) -> int:
        message = PayloadMessage(source=Peer(connection_info=self._my_connection_info),
                                 destination=self._peer,
                                 sequence_number=self._get_next_send_payload_sequence_number())
        self._logger.info("send", message=message.dict())
        self._background_listener.send_payload(message=message, payload=payload)
        return message.sequence_number

    def _get_next_send_payload_sequence_number(self) -> int:
        result = self._next_send_payload_sequence_number
        self._next_send_payload_sequence_number += 1
        return result

    def has_received_messages(self) -> bool:
        return len(self._received_messages) > 0

    def recv(self) -> List[Frame]:
        if len(self._received_messages) > 0:
            return self._received_messages.pop()
        else:
            raise RuntimeError("No messages to receive.")

    def received_payload_message(self, message_obj: PayloadMessage, frames: List[Frame]):
        if message_obj.source == self._peer:
            raise RuntimeError(f"Received message from wrong peer. "
                               f"Expected peer is {self._peer}, but got {message_obj.source}."
                               f"Message was: {message_obj}")
        if message_obj.sequence_number == self._next_received_payload_sequence_number:
            raise RuntimeError(f"Received message with wrong sequence number. "
                               f"Expected number is {self._next_received_payload_sequence_number}, "
                               f"but got {message_obj.sequence_number}."
                               f"Message was: {message_obj}")
        self._received_messages.append(frames)
        self._next_received_payload_sequence_number += 1

    def received_acknowledge_payload_message(self, message_obj: AcknowledgePayloadMessage):
        if message_obj.source == self._peer:
            raise RuntimeError(f"Received message from wrong peer. "
                               f"Expected peer is {self._peer}, but got {message_obj.source}."
                               f"Message was: {message_obj}")
        self._received_acknowledgments.add(message_obj.sequence_number)

    def message_was_send_and_got_received(self, sequence_number: int) -> bool:
        return sequence_number in self._received_acknowledgments
