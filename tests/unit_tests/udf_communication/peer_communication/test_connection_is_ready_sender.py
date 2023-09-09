import dataclasses
from typing import Union
from unittest.mock import MagicMock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_is_ready_sender import \
    ConnectionIsReadySender
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    out_control_socket_mock: Union[MagicMock, Socket]
    peer_is_ready_sender: ConnectionIsReadySender = None

    def reset_mock(self):
        self.out_control_socket_mock.reset_mock()


def create_test_setup():
    peer = Peer(
        connection_info=ConnectionInfo(
            name="t2",
            ipaddress=IPAddress(ip_address="127.0.0.1"),
            port=Port(port=12),
            group_identifier="g"
        ))
    my_connection_info = ConnectionInfo(
        name="t1",
        ipaddress=IPAddress(ip_address="127.0.0.1"),
        port=Port(port=11),
        group_identifier="g"
    )
    out_control_socket_mock = create_autospec(Socket)
    connection_is_ready_sender = ConnectionIsReadySender(
        peer=peer,
        my_connection_info=my_connection_info,
        out_control_socket=out_control_socket_mock,
    )
    return TestSetup(
        peer=peer,
        out_control_socket_mock=out_control_socket_mock,
        peer_is_ready_sender=connection_is_ready_sender
    )


def test_init():
    test_setup = create_test_setup()
    assert test_setup.out_control_socket_mock.mock_calls == []


def test_try_send_after_init():
    test_setup = create_test_setup()
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert test_setup.out_control_socket_mock.mock_calls == []


def test_try_send_after_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.peer_is_ready_sender.received_synchronize_connection()
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert test_setup.out_control_socket_mock.mock_calls == []


def test_try_send_after_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.peer_is_ready_sender.received_acknowledge_connection()
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert test_setup.out_control_socket_mock.mock_calls == [
        call.send(serialize_message(messages.ConnectionIsReady(peer=test_setup.peer)))
    ]


def test_try_send_after_synchronize_connection_and_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.peer_is_ready_sender.received_synchronize_connection()
    test_setup.peer_is_ready_sender.received_acknowledge_connection()
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(messages.ConnectionIsReady(peer=test_setup.peer)))
            ]
    )


def test_try_send_twice_after_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.peer_is_ready_sender.received_synchronize_connection()
    test_setup.peer_is_ready_sender.try_send()
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert test_setup.out_control_socket_mock.mock_calls == []


def test_try_send_twice_after_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.peer_is_ready_sender.received_acknowledge_connection()
    test_setup.peer_is_ready_sender.try_send()
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert test_setup.out_control_socket_mock.mock_calls == []
