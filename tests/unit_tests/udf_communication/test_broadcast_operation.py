import dataclasses
from typing import Union, List, Optional
from unittest.mock import MagicMock, create_autospec, call, Mock

from polyfactory.factories.pydantic_factory import ModelFactory

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.broadcast_operation import BroadcastOperation
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, Frame
from tests.mock_cast import mock_cast


@dataclasses.dataclass(frozen=True)
class Fixture:
    sequence_number: int
    value: bytes
    localhost_communicator_mock: Union[MagicMock, PeerCommunicator]
    multi_node_communicator_mock: Union[MagicMock, PeerCommunicator]
    socket_factory_mock: Union[MagicMock, SocketFactory]
    broadcast_operation: BroadcastOperation

    def reset_mocks(self):
        self.localhost_communicator_mock.reset_mock()
        self.socket_factory_mock.reset_mock()
        self.multi_node_communicator_mock.reset_mock()


def create_setup(value: Optional[bytes]) -> Fixture:
    sequence_number = 0
    localhost_communicator_mock: Union[MagicMock, PeerCommunicator] = create_autospec(PeerCommunicator)
    multi_node_communicator_mock: Union[MagicMock, PeerCommunicator] = create_autospec(PeerCommunicator)
    socket_factory_mock: Union[MagicMock, SocketFactory] = create_autospec(SocketFactory)
    broadcast_operation = BroadcastOperation(
        sequence_number=sequence_number,
        value=value,
        localhost_communicator=localhost_communicator_mock,
        multi_node_communicator=multi_node_communicator_mock,
        socket_factory=socket_factory_mock
    )
    test_setup = Fixture(
        sequence_number=sequence_number,
        value=value,
        localhost_communicator_mock=localhost_communicator_mock,
        multi_node_communicator_mock=multi_node_communicator_mock,
        socket_factory_mock=socket_factory_mock,
        broadcast_operation=broadcast_operation
    )
    return test_setup


def test_init():
    test_setup = create_setup(value=None)
    assert (
            test_setup.multi_node_communicator_mock.mock_calls == []
            and test_setup.localhost_communicator_mock.mock_calls == []
            and test_setup.socket_factory_mock.mock_calls == []
    )


def test_call_localhost_rank_greater_zero():
    test_setup = create_setup(value=None)
    expected_value = b"0"
    test_setup.reset_mocks()
    test_setup.localhost_communicator_mock.rank = 1
    peer = ModelFactory.create_factory(Peer).build()
    leader = ModelFactory.create_factory(Peer).build()
    test_setup.localhost_communicator_mock.peer = peer
    test_setup.localhost_communicator_mock.leader = leader
    frames: List[Union[Frame, MagicMock]] = [create_autospec(Frame), create_autospec(Frame)]
    mock_cast(frames[0].to_bytes).return_value = serialize_message(messages.Broadcast(
        source=leader,
        destination=peer,
        sequence_number=test_setup.sequence_number,
    ))
    mock_cast(frames[1].to_bytes).return_value = expected_value
    mock_cast(test_setup.localhost_communicator_mock.recv).side_effect = [frames]
    result = test_setup.broadcast_operation()
    assert result == expected_value \
           and mock_cast(test_setup.localhost_communicator_mock.recv).mock_calls == [call(peer=leader)] \
           and test_setup.socket_factory_mock.mock_calls == [] \
           and test_setup.multi_node_communicator_mock.mock_calls == []


def test_call_localhost_rank_equal_zero_multi_node_rank_greater_zero():
    test_setup = create_setup(value=None)
    expected_value = b"0"
    test_setup.reset_mocks()
    frame_mocks = [Mock()]
    mock_cast(test_setup.socket_factory_mock.create_frame).side_effect = frame_mocks
    test_setup.localhost_communicator_mock.rank = 0
    test_setup.multi_node_communicator_mock.rank = 1
    multi_node_peer = ModelFactory.create_factory(Peer).build()
    multi_node_leader = ModelFactory.create_factory(Peer).build()
    localhost_peer = ModelFactory.create_factory(Peer).build()
    localhost_leader = ModelFactory.create_factory(Peer).build()
    test_setup.localhost_communicator_mock.leader = localhost_leader
    test_setup.multi_node_communicator_mock.leader = multi_node_leader
    frames: List[Union[Frame, MagicMock]] = [create_autospec(Frame), create_autospec(Frame)]
    mock_cast(frames[0].to_bytes).return_value = serialize_message(messages.Broadcast(
        source=multi_node_leader,
        destination=multi_node_peer,
        sequence_number=test_setup.sequence_number,
    ))
    mock_cast(frames[1].to_bytes).return_value = expected_value
    mock_cast(test_setup.multi_node_communicator_mock.recv).side_effect = [frames]
    mock_cast(test_setup.localhost_communicator_mock.peers).return_value = [localhost_leader, localhost_peer]
    result = test_setup.broadcast_operation()
    assert result == expected_value \
           and mock_cast(test_setup.localhost_communicator_mock.send).mock_calls == [
               call(peer=localhost_peer, message=[frame_mocks[0], frames[1]])
           ] \
           and mock_cast(test_setup.multi_node_communicator_mock.recv).mock_calls == [
               call(multi_node_leader)
           ] \
           and mock_cast(test_setup.socket_factory_mock.create_frame).mock_calls == [
               call(serialize_message(
                   messages.Broadcast(
                       source=localhost_leader,
                       destination=localhost_peer,
                       sequence_number=0
                   )
               ))
           ]


def test_call_localhost_rank_equal_zero_multi_node_rank_equal_zero_multi_node_number_of_peers_one():
    test_setup = create_setup(value=b"0")
    test_setup.reset_mocks()
    test_setup.localhost_communicator_mock.rank = 0
    test_setup.multi_node_communicator_mock.rank = 0
    multi_node_leader = ModelFactory.create_factory(Peer).build()
    localhost_peer = ModelFactory.create_factory(Peer).build()
    localhost_leader = ModelFactory.create_factory(Peer).build()
    test_setup.localhost_communicator_mock.leader = localhost_leader
    test_setup.multi_node_communicator_mock.leader = multi_node_leader
    frame_mocks: List[Union[Frame, MagicMock]] = [create_autospec(Frame), create_autospec(Frame)]
    mock_cast(test_setup.socket_factory_mock.create_frame).side_effect = frame_mocks
    mock_cast(frame_mocks[0].to_bytes).return_value = serialize_message(messages.Broadcast(
        source=localhost_leader,
        destination=localhost_peer,
        sequence_number=test_setup.sequence_number,
    ))
    mock_cast(frame_mocks[1].to_bytes).return_value = test_setup.value
    mock_cast(test_setup.localhost_communicator_mock.peers).return_value = [localhost_leader, localhost_peer]
    mock_cast(test_setup.multi_node_communicator_mock.peers).return_value = [multi_node_leader]
    result = test_setup.broadcast_operation()
    assert result == test_setup.value \
           and mock_cast(test_setup.localhost_communicator_mock.send).mock_calls == [
               call(peer=localhost_peer, message=[frame_mocks[1], frame_mocks[0]])
           ] \
           and mock_cast(test_setup.multi_node_communicator_mock.peers).mock_calls == [call()] \
           and mock_cast(test_setup.socket_factory_mock.create_frame).mock_calls == [
               call(b'0'),
               call(serialize_message(
                   messages.Broadcast(
                       source=localhost_leader,
                       destination=localhost_peer,
                       sequence_number=0
                   )
               ))
           ]


def test_call_localhost_rank_equal_zero_multi_node_rank_equal_zero_multi_node_number_of_peers_two():
    test_setup = create_setup(value=b"0")
    test_setup.reset_mocks()
    test_setup.localhost_communicator_mock.rank = 0
    test_setup.multi_node_communicator_mock.rank = 0
    multi_node_leader = ModelFactory.create_factory(Peer).build()
    multi_node_peer = ModelFactory.create_factory(Peer).build()
    localhost_leader = ModelFactory.create_factory(Peer).build()
    test_setup.localhost_communicator_mock.leader = localhost_leader
    test_setup.multi_node_communicator_mock.leader = multi_node_leader
    frame_mocks: List[Union[Frame, MagicMock]] = [create_autospec(Frame), create_autospec(Frame)]
    mock_cast(test_setup.socket_factory_mock.create_frame).side_effect = frame_mocks
    mock_cast(frame_mocks[0].to_bytes).return_value = serialize_message(messages.Broadcast(
        source=multi_node_leader,
        destination=multi_node_peer,
        sequence_number=test_setup.sequence_number,
    ))
    mock_cast(frame_mocks[1].to_bytes).return_value = test_setup.value
    mock_cast(test_setup.localhost_communicator_mock.peers).return_value = [localhost_leader]
    mock_cast(test_setup.multi_node_communicator_mock.peers).return_value = [multi_node_leader, multi_node_peer]
    result = test_setup.broadcast_operation()
    assert result == test_setup.value \
           and mock_cast(test_setup.multi_node_communicator_mock.send).mock_calls == [
               call(peer=multi_node_peer, message=[frame_mocks[1], frame_mocks[0]])
           ] \
           and mock_cast(test_setup.localhost_communicator_mock.peers).mock_calls == [call()] \
           and mock_cast(test_setup.socket_factory_mock.create_frame).mock_calls == [
               call(b'0'),
               call(serialize_message(
                   messages.Broadcast(
                       source=multi_node_leader,
                       destination=multi_node_peer,
                       sequence_number=0
                   )
               ))
           ]
