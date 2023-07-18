import dataclasses
from typing import Union, cast, Any
from unittest.mock import MagicMock, Mock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket, \
    SocketFactory, SocketType


def mock_cast(obj: Any) -> Mock:
    return cast(Mock, obj)


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    my_connection_info: ConnectionInfo
    socket_factory_mock: Union[MagicMock, SocketFactory]
    receive_socket_mock: Union[MagicMock, Socket]
    sender_mock: Union[MagicMock, Sender]
    abort_timeout_sender_mock: Union[MagicMock, AbortTimeoutSender]
    peer_is_ready_sender_mock: Union[MagicMock, PeerIsReadySender]
    synchronize_connection_sender_mock: Union[MagicMock, SynchronizeConnectionSender]
    register_peer_sender: Union[MagicMock, RegisterPeerSender]
    register_peer_connection: Union[MagicMock, RegisterPeerConnection]
    acknowledge_register_peer_sender: Union[MagicMock, AcknowledgeRegisterPeerSender]
    background_peer_state: BackgroundPeerState

    def reset_mocks(self):
        mocks = (
            self.abort_timeout_sender_mock,
            self.synchronize_connection_sender_mock,
            self.peer_is_ready_sender_mock,
            self.sender_mock,
            self.receive_socket_mock,
            self.socket_factory_mock,
            self.acknowledge_register_peer_sender,
            self.register_peer_connection,
            self.register_peer_sender
        )
        for mock in mocks:
            mock.reset_mock()


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
    receive_socket_mock = create_autospec(Socket)
    socket_factory_mock: Union[MagicMock, SocketFactory] = create_autospec(SocketFactory)
    mock_cast(socket_factory_mock.create_socket).side_effect = [receive_socket_mock]
    sender_mock = create_autospec(Sender)
    abort_timeout_sender_mock = create_autospec(AbortTimeoutSender)
    peer_is_ready_sender_mock = create_autospec(PeerIsReadySender)
    synchronize_connection_sender_mock = create_autospec(SynchronizeConnectionSender)
    register_peer_sender_mock = create_autospec(RegisterPeerSender)
    register_peer_connection_mock = create_autospec(RegisterPeerConnection)
    acknowledge_register_peer_sender_mock = create_autospec(AcknowledgeRegisterPeerSender)
    background_peer_state = BackgroundPeerState(
        my_connection_info=my_connection_info,
        peer=peer,
        socket_factory=socket_factory_mock,
        sender=sender_mock,
        abort_timeout_sender=abort_timeout_sender_mock,
        peer_is_ready_sender=peer_is_ready_sender_mock,
        synchronize_connection_sender=synchronize_connection_sender_mock,
        register_peer_sender=register_peer_sender_mock,
        register_peer_connection=register_peer_connection_mock,
        acknowledge_register_peer_sender=acknowledge_register_peer_sender_mock,
        acknowledge_register_peer=False,
        forward_register_peer=False,
        needs_register_peer_complete=False
    )
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        socket_factory_mock=socket_factory_mock,
        sender_mock=sender_mock,
        abort_timeout_sender_mock=abort_timeout_sender_mock,
        peer_is_ready_sender_mock=peer_is_ready_sender_mock,
        synchronize_connection_sender_mock=synchronize_connection_sender_mock,
        background_peer_state=background_peer_state,
        receive_socket_mock=receive_socket_mock,
        register_peer_sender=register_peer_sender_mock,
        register_peer_connection=register_peer_connection_mock,
        acknowledge_register_peer_sender=acknowledge_register_peer_sender_mock
    )


def test_init():
    test_setup = create_test_setup()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == [call.try_send(force=True)]
            and test_setup.peer_is_ready_sender_mock.mock_calls == []
            and test_setup.abort_timeout_sender_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and test_setup.register_peer_sender.mock_calls == [call.try_send(force=True)]
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == [call.try_send(force=True)]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == [call(SocketType.PAIR)]
            and test_setup.receive_socket_mock.mock_calls == [
                call.bind('inproc://peer/g/127.0.0.1/11')
            ]
    )


def test_resend():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.resend_if_necessary()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == [call.try_send()]
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.try_send()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.try_send()]
            and test_setup.sender_mock.mock_calls == []
            and test_setup.register_peer_sender.mock_calls == [call.try_send()]
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == [call.try_send()]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_received_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_synchronize_connection()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == []
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.received_synchronize_connection(),
                                                                    call.reset_timer()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.received_synchronize_connection()]
            and test_setup.register_peer_sender.mock_calls == []
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == []
            and test_setup.sender_mock.mock_calls == [
                call.send(
                    messages.Message(__root__=messages.AcknowledgeConnection(source=test_setup.my_connection_info)))]
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_received_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_acknowledge_connection()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == [call.stop()]
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.received_acknowledge_connection()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.received_acknowledge_connection()]
            and test_setup.register_peer_sender.mock_calls == []
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_received_acknowledge_register_peer():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_acknowledge_register_peer()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == []
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.received_acknowledge_register_peer(),
                                                                    call.reset_timer()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.received_acknowledge_register_peer()]
            and test_setup.register_peer_sender.mock_calls == [call.stop()]
            and test_setup.register_peer_connection.mock_calls == [call.complete(test_setup.peer)]
            and test_setup.acknowledge_register_peer_sender.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )


def test_received_register_peer_complete():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_register_peer_complete()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == []
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.received_register_peer_complete()]
            and test_setup.abort_timeout_sender_mock.mock_calls == []
            and test_setup.register_peer_sender.mock_calls == []
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == [call.stop()]
            and test_setup.sender_mock.mock_calls == []
            and mock_cast(test_setup.socket_factory_mock.create_socket).mock_calls == []
            and test_setup.receive_socket_mock.mock_calls == []
    )
