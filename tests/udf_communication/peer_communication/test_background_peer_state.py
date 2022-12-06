import dataclasses
from typing import Union, cast, Any, List, Iterator
from unittest.mock import MagicMock, Mock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import AreYouReadyToReceiveMessage, \
    AckReadyToReceiveMessage, WeAreReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState, Clock
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket, \
    SocketFactory, SocketType


def mock_cast(obj: Any) -> Mock:
    return cast(Mock, obj)


@dataclasses.dataclass()
class TestSetup:
    send_socket_count: int
    time_steps: Iterator[int]
    reminder_timeout_in_ms: int
    peer: Peer = Peer(
        connection_info=ConnectionInfo(
            name="t1",
            ipaddress=IPAddress(ip_address="127.0.0.1"),
            port=Port(port=11),
            group_identifier="g"
        ))
    my_connection_info = ConnectionInfo(
        name="t0",
        ipaddress=IPAddress(ip_address="127.0.0.1"),
        port=Port(port=10),
        group_identifier="g"
    )
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    out_control_socket_mock: Union[MagicMock, Socket] = create_autospec(Socket)
    socket_factory_mock: Union[MagicMock, SocketFactory] = create_autospec(SocketFactory)
    receive_socket_mock: Union[MagicMock, Socket] = create_autospec(Socket)
    send_socket_mocks: List[Union[MagicMock, Socket]] = None
    background_peer_state = None

    def __post_init__(self):
        self.send_socket_mocks = [create_autospec(Socket) for i in range(self.send_socket_count)]
        mock_cast(self.socket_factory_mock.create_socket).side_effect = \
            [self.receive_socket_mock] + self.send_socket_mocks
        mock_cast(self.clock_mock.get_current_timestamp_in_ms).side_effect = list(self.time_steps)
        self.background_peer_state = BackgroundPeerState(
            my_connection_info=self.my_connection_info,
            peer=self.peer,
            socket_factory=self.socket_factory_mock,
            out_control_socket=self.out_control_socket_mock,
            clock=self.clock_mock,
            reminder_timeout_in_ms=self.reminder_timeout_in_ms
        )

    def reset_mock(self):
        for send_socket in self.send_socket_mocks:
            send_socket.reset_mock()
        self.socket_factory_mock.reset_mock()
        self.receive_socket_mock.reset_mock()
        self.clock_mock.reset_mock()
        self.out_control_socket_mock.reset_mock()


def test_init():
    test_setup = TestSetup(send_socket_count=1, time_steps=range(1), reminder_timeout_in_ms=1)
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(AreYouReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [
                call(SocketType.PAIR),
                call(SocketType.DEALER)
            ]
            and test_setup.receive_socket_mock.mock_calls == [
                call.bind('inproc://peer/g/127.0.0.1/11')
            ]
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 1
    )


def test_resend_not_necassary():
    test_setup = TestSetup(send_socket_count=1, time_steps=range(2), reminder_timeout_in_ms=10)
    test_setup.reset_mock()
    test_setup.background_peer_state.resend_if_necessary()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 1
    )


def test_resend_necessary():
    test_setup = TestSetup(send_socket_count=2, time_steps=[0, 3, 4], reminder_timeout_in_ms=1)
    test_setup.reset_mock()
    test_setup.background_peer_state.resend_if_necessary()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(AreYouReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [
                call(SocketType.DEALER)
            ]
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 2
    )


def test_received_are_you_ready_to_receive():
    test_setup = TestSetup(send_socket_count=2, time_steps=range(2), reminder_timeout_in_ms=1)
    test_setup.reset_mock()
    test_setup.background_peer_state.received_are_you_ready_to_receive()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(WeAreReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [
                call(SocketType.DEALER)
            ]
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 1
    )


def test_received_are_you_ready_to_receive_and_resend_not_necessary():
    test_setup = TestSetup(send_socket_count=2, time_steps=range(4), reminder_timeout_in_ms=10)
    test_setup.background_peer_state.received_are_you_ready_to_receive()
    test_setup.reset_mock()
    test_setup.background_peer_state.resend_if_necessary()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 2
    )


def test_received_are_you_ready_to_receive_and_resend_necessary():
    test_setup = TestSetup(send_socket_count=4, time_steps=[0, 0, 3, 4, 5, 6], reminder_timeout_in_ms=1)
    test_setup.background_peer_state.received_are_you_ready_to_receive()
    test_setup.reset_mock()
    test_setup.background_peer_state.resend_if_necessary()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == []
            and test_setup.send_socket_mocks[2].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(AreYouReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and test_setup.send_socket_mocks[3].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(WeAreReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [
                call(SocketType.DEALER),
                call(SocketType.DEALER)
            ]
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 4
    )


def test_received_we_are_ready_to_receive():
    test_setup = TestSetup(send_socket_count=2, time_steps=range(1), reminder_timeout_in_ms=1)
    test_setup.reset_mock()
    test_setup.background_peer_state.received_we_are_ready_to_receive()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(AckReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [
                call(SocketType.DEALER)
            ]
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 0
    )


def test_received_we_are_ready_to_receive_resend_not_necessary():
    test_setup = TestSetup(send_socket_count=2, time_steps=range(2), reminder_timeout_in_ms=10)
    test_setup.background_peer_state.received_we_are_ready_to_receive()
    test_setup.reset_mock()
    test_setup.background_peer_state.resend_if_necessary()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 1
    )


def test_received_we_are_ready_to_receive_resend_necessary():
    test_setup = TestSetup(send_socket_count=2, time_steps=[0, 3], reminder_timeout_in_ms=1)
    test_setup.background_peer_state.received_we_are_ready_to_receive()
    test_setup.reset_mock()
    test_setup.background_peer_state.resend_if_necessary()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 1
    )


def test_received_we_are_ready_to_receive_and_received_are_you_ready_to_receive():
    test_setup = TestSetup(send_socket_count=3, time_steps=range(2), reminder_timeout_in_ms=1)
    test_setup.reset_mock()
    test_setup.background_peer_state.received_we_are_ready_to_receive()
    test_setup.background_peer_state.received_are_you_ready_to_receive()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(AckReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and test_setup.send_socket_mocks[2].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(WeAreReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [
                call(SocketType.DEALER),
                call(SocketType.DEALER)
            ]
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 1
    )


def test_received_we_are_ready_to_receive_and_received_are_you_ready_to_receive_resend_necessary():
    test_setup = TestSetup(send_socket_count=4, time_steps=[0, 0, 3, 3, 4], reminder_timeout_in_ms=1)
    test_setup.background_peer_state.received_we_are_ready_to_receive()
    test_setup.background_peer_state.received_are_you_ready_to_receive()
    test_setup.reset_mock()
    test_setup.background_peer_state.resend_if_necessary()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == []
            and test_setup.send_socket_mocks[2].mock_calls == []
            and test_setup.send_socket_mocks[3].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(WeAreReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [
                call(SocketType.DEALER),
            ]
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 3
    )


def test_two_received_are_you_ready_to_receive_resend_necessary():
    test_setup = TestSetup(send_socket_count=3, time_steps=[0, 0, 0, 0, 0], reminder_timeout_in_ms=1)
    test_setup.background_peer_state.received_are_you_ready_to_receive()
    test_setup.reset_mock()
    test_setup.background_peer_state.received_are_you_ready_to_receive()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == []
            and test_setup.send_socket_mocks[2].mock_calls == [
                call.connect('tcp://127.0.0.1:11'),
                call.send(serialize_message(WeAreReadyToReceiveMessage(source=test_setup.my_connection_info))),
                call.close(linger=0)
            ]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [
                call(SocketType.DEALER),
            ]
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 2
    )


def test_received_are_you_ready_to_receive_and_received_ack():
    test_setup = TestSetup(send_socket_count=3, time_steps=[0, 0, 0, 0, 0, 3], reminder_timeout_in_ms=1)
    test_setup.background_peer_state.received_are_you_ready_to_receive()
    test_setup.reset_mock()
    test_setup.background_peer_state.received_ack()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.send_socket_mocks[0].mock_calls == []
            and test_setup.send_socket_mocks[1].mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
            and len(mock_cast(test_setup.clock_mock.get_current_timestamp_in_ms).mock_calls) == 0
    )
