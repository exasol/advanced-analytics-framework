import dataclasses
from typing import Union, cast, Any
from unittest.mock import MagicMock, Mock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket, \
    SocketFactory, SocketType, Frame


def mock_cast(obj: Any) -> Mock:
    return cast(Mock, obj)


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    my_connection_info: ConnectionInfo
    socket_factory_mock: Union[MagicMock, SocketFactory]
    receive_socket_mock: Union[MagicMock, Socket]
    sender_mock: Union[MagicMock, Sender]
    connection_establisher_mock: Union[MagicMock, ConnectionEstablisher]
    background_peer_state: BackgroundPeerState

    def reset_mocks(self):
        self.sender_mock.reset_mock()
        self.receive_socket_mock.reset_mock()
        self.socket_factory_mock.reset_mock()
        self.connection_establisher_mock.reset_mock()


def create_test_setup() -> TestSetup:
    peer = Peer(
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
    receive_socket_mock: Union[MagicMock, Socket] = create_autospec(Socket)
    socket_factory_mock: Union[MagicMock, SocketFactory] = create_autospec(SocketFactory)
    mock_cast(socket_factory_mock.create_socket).side_effect = [receive_socket_mock]
    sender_mock: Union[MagicMock, Sender] = create_autospec(Sender)
    connection_establisher_mock: Union[MagicMock, ConnectionEstablisher] = create_autospec(ConnectionEstablisher)

    background_peer_state = BackgroundPeerState(
        my_connection_info=my_connection_info,
        peer=peer,
        socket_factory=socket_factory_mock,
        sender=sender_mock,
        connection_establisher=connection_establisher_mock
    )
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        socket_factory_mock=socket_factory_mock,
        sender_mock=sender_mock,
        background_peer_state=background_peer_state,
        receive_socket_mock=receive_socket_mock,
        connection_establisher_mock=connection_establisher_mock
    )


def test_init():
    test_setup = create_test_setup()
    assert (
            test_setup.sender_mock.mock_calls == []
            and test_setup.connection_establisher_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [call(SocketType.PAIR)]
            and test_setup.receive_socket_mock.mock_calls == [
                call.bind('inproc://peer/g/127.0.0.1/11')
            ]
    )


def test_resend_if_necessary():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.try_send()
    assert (
            test_setup.connection_establisher_mock.mock_calls == [call.try_send()]
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_received_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_synchronize_connection()
    assert (
            test_setup.connection_establisher_mock.mock_calls == [call.received_synchronize_connection()]
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_received_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_acknowledge_connection()
    assert (
            test_setup.connection_establisher_mock.mock_calls == [call.received_acknowledge_connection()]
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_received_acknowledge_register_peer():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_acknowledge_register_peer()
    assert (
            test_setup.connection_establisher_mock.mock_calls == [call.received_acknowledge_register_peer()]
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_received_register_peer_complete():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_register_peer_complete()
    assert (
            test_setup.connection_establisher_mock.mock_calls == [call.received_register_peer_complete()]
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_forward_payload():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    frames = [create_autospec(Frame)]
    test_setup.background_peer_state.forward_payload(frames=frames)
    assert (
            test_setup.connection_establisher_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == [call.send_multipart(frames)]
    )


def test_close():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.stop()
    assert (
            test_setup.connection_establisher_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == [call.close(linger=0)]
    )
