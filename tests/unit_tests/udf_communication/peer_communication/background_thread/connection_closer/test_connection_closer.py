import dataclasses
from typing import Union
from unittest.mock import MagicMock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import Message
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator. \
    background_thread.connection_closer.connection_closer import ConnectionCloser
from exasol_advanced_analytics_framework.udf_communication.peer_communicator. \
    background_thread.connection_closer.connection_is_closed_sender import ConnectionIsClosedSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    my_connection_info: ConnectionInfo
    sender_mock: Union[MagicMock, Sender]
    abort_timeout_sender_mock: Union[MagicMock, AbortTimeoutSender]
    connection_is_closed_sender_mock: Union[MagicMock, ConnectionIsClosedSender]
    close_connection_sender_mock: Union[MagicMock, SynchronizeConnectionSender]

    connection_closer: ConnectionCloser

    def reset_mock(self):
        self.abort_timeout_sender_mock.reset_mock()
        self.close_connection_sender_mock.reset_mock()
        self.connection_is_closed_sender_mock.reset_mock()
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
    connection_is_closed_sender: Union[MagicMock, ConnectionIsClosedSender] = create_autospec(ConnectionIsClosedSender)
    close_connection_sender_mock: Union[MagicMock, SynchronizeConnectionSender] = \
        create_autospec(SynchronizeConnectionSender)
    connection_closer = ConnectionCloser(
        my_connection_info=my_connection_info,
        peer=peer,
        sender=sender_mock,
        abort_timeout_sender=abort_timeout_sender_mock,
        connection_is_closed_sender=connection_is_closed_sender,
        close_connection_sender=close_connection_sender_mock,
    )
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        sender_mock=sender_mock,
        abort_timeout_sender_mock=abort_timeout_sender_mock,
        connection_is_closed_sender_mock=connection_is_closed_sender,
        close_connection_sender_mock=close_connection_sender_mock,
        connection_closer=connection_closer,
    )


def test_init():
    test_setup = create_test_setup()
    assert (
            test_setup.close_connection_sender_mock.mock_calls == [call.try_send(force=True)]
            and test_setup.connection_is_closed_sender_mock.mock_calls == []
            and test_setup.abort_timeout_sender_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
    )


def test_try_send():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_closer.try_send()
    assert (
            test_setup.close_connection_sender_mock.mock_calls == [call.try_send()]
            and test_setup.connection_is_closed_sender_mock.mock_calls == [call.try_send()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.try_send()]
            and test_setup.sender_mock.mock_calls == []
    )


def test_received_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_closer.received_close_connection()
    assert (
            test_setup.close_connection_sender_mock.mock_calls == []
            and test_setup.connection_is_closed_sender_mock.mock_calls == [call.received_close_connection()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.stop()]
            and test_setup.sender_mock.mock_calls == [
                call.send(Message(__root__=messages.AcknowledgeCloseConnection(source=test_setup.my_connection_info)))]
    )


def test_received_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_closer.received_acknowledge_close_connection()
    assert (
            test_setup.close_connection_sender_mock.mock_calls == [call.stop()]
            and test_setup.connection_is_closed_sender_mock.mock_calls == [call.received_acknowledge_close_connection()]
            and test_setup.abort_timeout_sender_mock.mock_calls == [call.stop()]
            and test_setup.sender_mock.mock_calls == []
    )
