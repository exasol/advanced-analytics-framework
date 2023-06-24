import dataclasses
from typing import Union, cast, Any
from unittest.mock import MagicMock, Mock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import PeerIsReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket


def mock_cast(obj: Any) -> Mock:
    return cast(Mock, obj)


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    timer_mock: Union[MagicMock, Timer]
    out_control_socket_mock: Union[MagicMock, Socket]
    peer_is_ready_sender: PeerIsReadySender = None

    def reset_mock(self):
        self.out_control_socket_mock.reset_mock()
        self.timer_mock.reset_mock()


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
    timer_mock = create_autospec(Timer)
    out_control_socket_mock = create_autospec(Socket)
    peer_is_ready_sender = PeerIsReadySender(
        peer=peer,
        my_connection_info=my_connection_info,
        out_control_socket=out_control_socket_mock,
        timer=timer_mock
    )
    return TestSetup(
        peer=peer,
        timer_mock=timer_mock,
        out_control_socket_mock=out_control_socket_mock,
        peer_is_ready_sender=peer_is_ready_sender
    )


def test_init():
    test_setup = create_test_setup()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == []
    )


def test_send_if_necessary_after_init_and_is_time_false():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.send_if_necessary()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


def test_send_if_necessary_after_init_and_is_time_false_and_force():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.send_if_necessary(force=True)

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(PeerIsReadyToReceiveMessage(peer=test_setup.peer)))
            ]
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


def test_send_if_necessary_after_init_and_is_time_true():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.send_if_necessary()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


def test_send_if_necessary_after_enable_and_is_time_false():
    test_setup = create_test_setup()
    test_setup.peer_is_ready_sender.enable()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.send_if_necessary()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_send_if_necessary_after_enable_and_is_time_true():
    test_setup = create_test_setup()
    test_setup.peer_is_ready_sender.enable()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.send_if_necessary()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(PeerIsReadyToReceiveMessage(peer=test_setup.peer)))
            ]
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_send_if_necessary_after_enable_and_is_time_true_twice():
    test_setup = create_test_setup()
    test_setup.peer_is_ready_sender.enable()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.peer_is_ready_sender.send_if_necessary()
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.send_if_necessary()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_reset_timer():
    test_setup = create_test_setup()
    print(test_setup.timer_mock.mock_calls)
    test_setup.peer_is_ready_sender.reset_timer()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.reset_timer()]
    )
