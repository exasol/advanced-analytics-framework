import dataclasses
from typing import Union
from unittest.mock import MagicMock, create_autospec, call, Mock

from polyfactory.factories.pydantic_factory import ModelFactory

from exasol_advanced_analytics_framework.udf_communication.gather_operation import GatherOperation
from exasol_advanced_analytics_framework.udf_communication.messages import Gather
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, Frame
from tests.mock_cast import mock_cast


@dataclasses.dataclass(frozen=True)
class TestSetup:
    sequence_number: int
    value: bytes
    number_of_instances_per_node: int
    localhost_communicator_mock: Union[MagicMock, PeerCommunicator]
    multi_node_communicator_mock: Union[MagicMock, PeerCommunicator]
    socket_factory_mock: Union[MagicMock, SocketFactory]
    gather_operation: GatherOperation

    def reset_mocks(self):
        self.localhost_communicator_mock.reset_mock()
        self.socket_factory_mock.reset_mock()
        self.multi_node_communicator_mock.reset_mock()


def create_setup(number_of_instances_per_node: int) -> TestSetup:
    sequence_number = 0
    value = "0".encode("utf-8")
    localhost_communicator_mock: Union[MagicMock, PeerCommunicator] = create_autospec(PeerCommunicator)
    multi_node_communicator_mock: Union[MagicMock, PeerCommunicator] = create_autospec(PeerCommunicator)
    socket_factory_mock: Union[MagicMock, SocketFactory] = create_autospec(SocketFactory)
    gather_operation = GatherOperation(
        sequence_number=sequence_number,
        value=value,
        number_of_instances_per_node=number_of_instances_per_node,
        localhost_communicator=localhost_communicator_mock,
        multi_node_communicator=multi_node_communicator_mock,
        socket_factory=socket_factory_mock
    )
    test_setup = TestSetup(
        sequence_number=sequence_number,
        value=value,
        number_of_instances_per_node=number_of_instances_per_node,
        localhost_communicator_mock=localhost_communicator_mock,
        multi_node_communicator_mock=multi_node_communicator_mock,
        socket_factory_mock=socket_factory_mock,
        gather_operation=gather_operation
    )
    return test_setup


def test_init():
    test_setup = create_setup(number_of_instances_per_node=2)
    assert test_setup.multi_node_communicator_mock.mock_calls == [] \
           and test_setup.localhost_communicator_mock.mock_calls == [] \
           and test_setup.socket_factory_mock.mock_calls == []


def test_call_localhost_rank_greater_zero():
    test_setup = create_setup(number_of_instances_per_node=2)
    test_setup.reset_mocks()
    frame_mocks = [Mock(), Mock()]
    mock_cast(test_setup.socket_factory_mock.create_frame).side_effect = frame_mocks
    test_setup.localhost_communicator_mock.rank = 1
    peer = ModelFactory.create_factory(Peer).build()
    leader = ModelFactory.create_factory(Peer).build()
    test_setup.localhost_communicator_mock.peer = peer
    test_setup.localhost_communicator_mock.leader = leader
    result = test_setup.gather_operation()
    assert result is None \
           and mock_cast(test_setup.localhost_communicator_mock.send).mock_calls == [
               call(peer=leader, message=[frame_mocks[1], frame_mocks[0]])
           ] and mock_cast(test_setup.socket_factory_mock.create_frame).mock_calls == [
               call(test_setup.value),
               call(serialize_message(
                   Gather(
                       source=peer,
                       destination=leader,
                       position=1,
                       sequence_number=test_setup.sequence_number,
                   )
               ))
           ] and test_setup.multi_node_communicator_mock.mock_calls == []


def test_call_localhost_rank_equal_zero_multi_node_rank_greater_zero():
    test_setup = create_setup(number_of_instances_per_node=2)
    test_setup.reset_mocks()
    frame_mocks = [Mock(), Mock(), Mock()]
    mock_cast(test_setup.socket_factory_mock.create_frame).side_effect = frame_mocks
    test_setup.localhost_communicator_mock.rank = 0
    test_setup.multi_node_communicator_mock.rank = 1
    multi_node_peer = ModelFactory.create_factory(Peer).build()
    multi_node_leader = ModelFactory.create_factory(Peer).build()
    localhost_peer = ModelFactory.create_factory(Peer).build()
    localhost_leader = ModelFactory.create_factory(Peer).build()
    test_setup.multi_node_communicator_mock.peer = multi_node_peer
    test_setup.localhost_communicator_mock.peer = localhost_leader
    recv_message_frame_mock: Union[MagicMock, Frame] = create_autospec(Frame)
    mock_cast(recv_message_frame_mock.to_bytes).return_value = serialize_message(Gather(
        source=localhost_peer,
        destination=localhost_leader,
        sequence_number=test_setup.sequence_number,
        position=1
    ))
    recv_value_frame_mock: Union[MagicMock, Frame] = create_autospec(Frame)
    mock_cast(test_setup.multi_node_communicator_mock.peers).return_value = [multi_node_leader, multi_node_peer]
    test_setup.multi_node_communicator_mock.leader = multi_node_leader
    mock_cast(test_setup.localhost_communicator_mock.peers).return_value = [localhost_leader, localhost_peer]
    mock_cast(test_setup.localhost_communicator_mock.poll_peers).return_value = [localhost_peer]
    mock_cast(test_setup.localhost_communicator_mock.recv).side_effect = [
        [recv_message_frame_mock, recv_value_frame_mock]]
    result = test_setup.gather_operation()
    assert result is None \
           and test_setup.localhost_communicator_mock.mock_calls == [
               call.peers(),
               call.poll_peers(),
               call.recv(localhost_peer)
           ] \
           and test_setup.multi_node_communicator_mock.mock_calls == [
               call.send(peer=multi_node_leader, message=[frame_mocks[1], frame_mocks[0]]),
               call.send(peer=multi_node_leader, message=[frame_mocks[2], recv_value_frame_mock])
           ] \
           and mock_cast(test_setup.socket_factory_mock.create_frame).mock_calls == [
               call(b'0'),
               call(serialize_message(
                   Gather(
                       source=multi_node_peer,
                       destination=multi_node_leader,
                       position=2,
                       sequence_number=0
                   )
               )),
               call(serialize_message(
                   Gather(
                       source=multi_node_peer,
                       destination=multi_node_leader,
                       position=3,
                       sequence_number=0
                   )
               )),
           ]


def test_call_localhost_rank_equal_zero_multi_node_rank_equal_zero_multi_node_number_of_peers_one():
    test_setup = create_setup(number_of_instances_per_node=2)
    test_setup.reset_mocks()
    test_setup.localhost_communicator_mock.rank = 0
    test_setup.multi_node_communicator_mock.rank = 0
    test_setup.multi_node_communicator_mock.number_of_peers = 1
    multi_node_peer = ModelFactory.create_factory(Peer).build()
    multi_node_leader = ModelFactory.create_factory(Peer).build()
    localhost_peer = ModelFactory.create_factory(Peer).build()
    localhost_leader = ModelFactory.create_factory(Peer).build()
    test_setup.multi_node_communicator_mock.peer = multi_node_peer
    test_setup.localhost_communicator_mock.peer = localhost_leader
    recv_message_frame_mock: Union[MagicMock, Frame] = create_autospec(Frame)
    mock_cast(recv_message_frame_mock.to_bytes).return_value = serialize_message(Gather(
        source=localhost_peer,
        destination=localhost_leader,
        sequence_number=test_setup.sequence_number,
        position=1
    ))
    recv_value_frame_mock: Union[MagicMock, Frame] = create_autospec(Frame)
    mock_cast(test_setup.localhost_communicator_mock.poll_peers).return_value = [localhost_peer]
    mock_cast(test_setup.localhost_communicator_mock.recv).side_effect = [
        [recv_message_frame_mock, recv_value_frame_mock]]
    result = test_setup.gather_operation()
    assert result is not None \
           and test_setup.localhost_communicator_mock.mock_calls == [
               call.poll_peers(),
               call.recv(localhost_peer)
           ] \
           and test_setup.multi_node_communicator_mock.mock_calls == [] \
           and mock_cast(test_setup.socket_factory_mock).mock_calls == []


def test_call_localhost_rank_equal_zero_multi_node_rank_equal_zero_multi_node_number_of_peers_two():
    test_setup = create_setup(number_of_instances_per_node=1)
    test_setup.reset_mocks()
    test_setup.localhost_communicator_mock.rank = 0
    test_setup.multi_node_communicator_mock.rank = 0
    test_setup.multi_node_communicator_mock.number_of_peers = 2
    multi_node_peer = ModelFactory.create_factory(Peer).build()
    multi_node_leader = ModelFactory.create_factory(Peer).build()
    test_setup.multi_node_communicator_mock.peer = multi_node_peer
    recv_message_frame_mock: Union[MagicMock, Frame] = create_autospec(Frame)
    mock_cast(recv_message_frame_mock.to_bytes).return_value = serialize_message(Gather(
        source=multi_node_peer,
        destination=multi_node_leader,
        sequence_number=test_setup.sequence_number,
        position=1
    ))
    recv_value_frame_mock: Union[MagicMock, Frame] = create_autospec(Frame)
    mock_cast(test_setup.multi_node_communicator_mock.peers).return_value = [multi_node_leader, multi_node_peer]
    test_setup.multi_node_communicator_mock.leader = multi_node_leader
    mock_cast(test_setup.multi_node_communicator_mock.poll_peers).return_value = [multi_node_peer]
    mock_cast(test_setup.multi_node_communicator_mock.recv).side_effect = [
        [recv_message_frame_mock, recv_value_frame_mock]]
    result = test_setup.gather_operation()
    assert result is not None \
           and test_setup.localhost_communicator_mock.mock_calls == [] \
           and test_setup.multi_node_communicator_mock.mock_calls == [
               call.poll_peers(),
               call.recv(multi_node_peer)
           ] \
           and mock_cast(test_setup.socket_factory_mock).mock_calls == []
