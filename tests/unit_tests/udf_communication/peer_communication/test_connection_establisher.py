import dataclasses
from typing import Union
from unittest.mock import MagicMock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import AcknowledgeConnection, Message
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_is_ready_sender import \
    ConnectionIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    my_connection_info: ConnectionInfo
    sender_mock: Union[MagicMock, Sender]
    abort_timeout_sender_mock: Union[MagicMock, AbortTimeoutSender]
    connection_is_ready_sender_mock: Union[MagicMock, ConnectionIsReadySender]
    synchronize_connection_sender_mock: Union[MagicMock, SynchronizeConnectionSender]

    connection_establisher: ConnectionEstablisher

    def reset_mock(self):
        self.abort_timeout_sender_mock.reset_mock()
        self.synchronize_connection_sender_mock.reset_mock()
        self.connection_is_ready_sender_mock.reset_mock()
        self.sender_mock.reset_mock()


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
    connection_is_ready_sender: Union[MagicMock, ConnectionIsReadySender] = create_autospec(ConnectionIsReadySender)
    synchronize_connection_sender_mock: Union[MagicMock, SynchronizeConnectionSender] = \
        create_autospec(SynchronizeConnectionSender)
    connection_establisher = ConnectionEstablisher(
        my_connection_info=my_connection_info,
        peer=peer,
        sender=sender_mock,
        abort_timeout_sender=abort_timeout_sender_mock,
        connection_is_ready_sender=connection_is_ready_sender,
        synchronize_connection_sender=synchronize_connection_sender_mock,
    )
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        sender_mock=sender_mock,
        abort_timeout_sender_mock=abort_timeout_sender_mock,
        connection_is_ready_sender_mock=connection_is_ready_sender,
        synchronize_connection_sender_mock=synchronize_connection_sender_mock,
        connection_establisher=connection_establisher,
    )


def test_init():
    test_setup = create_test_setup()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == [call.try_send(force=True)]
            and test_setup.connection_is_ready_sender_mock.mock_calls == []
            and test_setup.abort_timeout_sender_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
    )


def test_try_send():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher.try_send()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == [call.try_send()]
            and test_setup.connection_is_ready_sender_mock.mock_calls == [call.try_send()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.try_send()]
            and test_setup.sender_mock.mock_calls == []
    )


def test_received_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher.received_synchronize_connection()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == []
            and test_setup.connection_is_ready_sender_mock.mock_calls == [call.received_synchronize_connection()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.stop()]
            and test_setup.sender_mock.mock_calls == [
                call.send(Message(__root__=AcknowledgeConnection(source=test_setup.my_connection_info)))]
    )


def test_received_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher.received_acknowledge_connection()
    assert (
            test_setup.synchronize_connection_sender_mock.mock_calls == [call.stop()]
            and test_setup.connection_is_ready_sender_mock.mock_calls == [call.received_acknowledge_connection()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.stop()]
            and test_setup.sender_mock.mock_calls == []
    )
