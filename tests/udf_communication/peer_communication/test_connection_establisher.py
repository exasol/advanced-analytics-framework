import dataclasses
from typing import Union
from unittest.mock import MagicMock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import AcknowledgeConnectionMessage, Message
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    my_connection_info: ConnectionInfo
    sender_mock: Union[MagicMock, Sender]
    abort_timeout_sender_mock: Union[MagicMock, AbortTimeoutSender]
    peer_is_ready_sender_mock: Union[MagicMock, PeerIsReadySender]
    synchronize_connection_sender_mock: Union[MagicMock, SynchronizeConnectionSender]
    register_peer_sender: Union[MagicMock, RegisterPeerSender]
    register_peer_connection: Union[MagicMock, RegisterPeerConnection]
    acknowledge_register_peer_sender: Union[MagicMock, AcknowledgeRegisterPeerSender]
    connection_establisher: ConnectionEstablisher

    def reset_mock(self):
        self.abort_timeout_sender_mock.reset_mock()
        self.synchronize_connection_sender_mock.reset_mock()
        self.peer_is_ready_sender_mock.reset_mock()
        self.sender_mock.reset_mock()
        self.register_peer_sender.reset_mock()
        self.acknowledge_register_peer_sender.reset_mock()
        self.register_peer_connection.reset_mock()


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
    sender_mock: Union[MagicMock, Sender] = create_autospec(Sender)
    abort_timeout_sender_mock: Union[MagicMock, AbortTimeoutSender] = create_autospec(AbortTimeoutSender)
    peer_is_ready_sender_mock: Union[MagicMock, PeerIsReadySender] = create_autospec(PeerIsReadySender)
    synchronize_connection_sender_mock: Union[MagicMock, SynchronizeConnectionSender] = \
        create_autospec(SynchronizeConnectionSender)
    register_peer_sender_mock: Union[MagicMock, RegisterPeerSender] = create_autospec(RegisterPeerSender)
    register_peer_connection_mock: Union[MagicMock, RegisterPeerConnection] = create_autospec(RegisterPeerConnection)
    acknowledge_register_peer_sender_mock: Union[MagicMock, AcknowledgeRegisterPeerSender] = \
        create_autospec(AcknowledgeRegisterPeerSender)
    connection_establisher = ConnectionEstablisher(
        my_connection_info=my_connection_info,
        peer=peer,
        sender=sender_mock,
        abort_timeout_sender=abort_timeout_sender_mock,
        peer_is_ready_sender=peer_is_ready_sender_mock,
        synchronize_connection_sender=synchronize_connection_sender_mock,
        register_peer_sender=register_peer_sender_mock,
        register_peer_connection=register_peer_connection_mock,
        acknowledge_register_peer_sender=acknowledge_register_peer_sender_mock,
    )
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        sender_mock=sender_mock,
        abort_timeout_sender_mock=abort_timeout_sender_mock,
        peer_is_ready_sender_mock=peer_is_ready_sender_mock,
        synchronize_connection_sender_mock=synchronize_connection_sender_mock,
        connection_establisher=connection_establisher,
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
    )


def test_try_send():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher.try_send()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == [call.try_send()]
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.try_send()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.try_send()]
            and test_setup.sender_mock.mock_calls == []
            and test_setup.register_peer_sender.mock_calls == [call.try_send()]
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == [call.try_send()]
    )


def test_received_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher.received_synchronize_connection()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == []
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.received_synchronize_connection(),
                                                                    call.reset_timer()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.received_synchronize_connection()]
            and test_setup.register_peer_sender.mock_calls == []
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == []
            and test_setup.sender_mock.mock_calls == [
                call.send(Message(__root__=AcknowledgeConnectionMessage(source=test_setup.my_connection_info)))]
    )


def test_received_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher.received_acknowledge_connection()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == [call.stop()]
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.received_acknowledge_connection()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.received_acknowledge_connection()]
            and test_setup.register_peer_sender.mock_calls == []
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
    )


def test_received_acknowledge_register_peer():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher.received_acknowledge_register_peer()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == []
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.received_acknowledge_register_peer(),
                                                                    call.reset_timer()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.received_acknowledge_register_peer()]
            and test_setup.register_peer_sender.mock_calls == [call.stop()]
            and test_setup.register_peer_connection.mock_calls == [call.complete(test_setup.peer)]
            and test_setup.acknowledge_register_peer_sender.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
    )


def test_received_register_peer_complete():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher.received_register_peer_complete()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == []
            and test_setup.peer_is_ready_sender_mock.mock_calls == [call.received_register_peer_complete()]
            and test_setup.abort_timeout_sender_mock.mock_calls == []
            and test_setup.register_peer_sender.mock_calls == []
            and test_setup.register_peer_connection.mock_calls == []
            and test_setup.acknowledge_register_peer_sender.mock_calls == [call.stop()]
            and test_setup.sender_mock.mock_calls == []
    )
