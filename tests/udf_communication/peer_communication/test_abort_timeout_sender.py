import dataclasses
from typing import Union, cast, Any
from unittest.mock import MagicMock, Mock, create_autospec, call

import pytest

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket
from tests.udf_communication.test_messages import messages


def mock_cast(obj: Any) -> Mock:
    return cast(Mock, obj)


@dataclasses.dataclass()
class TestSetup:
    timer_mock: Union[MagicMock, Timer]
    out_control_socket_mock: Union[MagicMock, Socket]
    abort_timeout_sender: AbortTimeoutSender = None

    def reset_mock(self):
        self.out_control_socket_mock.reset_mock()
        self.timer_mock.reset_mock()


def create_test_setup(needs_acknowledge_register_peer: bool):
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
    abort_timeout_sender = AbortTimeoutSender(
        peer=peer,
        my_connection_info=my_connection_info,
        out_control_socket=out_control_socket_mock,
        timer=timer_mock,
        needs_acknowledge_register_peer=needs_acknowledge_register_peer
    )
    return TestSetup(
        timer_mock=timer_mock,
        out_control_socket_mock=out_control_socket_mock,
        abort_timeout_sender=abort_timeout_sender
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_init(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == []
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_init_and_is_time_false(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_init_and_is_time_true(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(messages.Timeout()))
            ]
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize(
    "needs_acknowledge_register_peer, is_time",
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ])
def test_try_send_twice_and_is_time_false(needs_acknowledge_register_peer: bool, is_time: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.abort_timeout_sender.try_send()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_acknowledge_register_peer_and_is_time_false(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.abort_timeout_sender.received_acknowledge_register_peer()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_acknowledge_register_peer_and_is_time_true(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.abort_timeout_sender.received_acknowledge_register_peer()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(Timeout()))
            ]
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_synchronize_connection_and_is_time_false(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.abort_timeout_sender.received_synchronize_connection()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_try_send_after_synchronize_connection_and_is_time_true_and_needs_acknowledge_register_peer_true():
    test_setup = create_test_setup(needs_acknowledge_register_peer=True)
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.abort_timeout_sender.received_synchronize_connection()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_try_send_after_synchronize_connection_and_is_time_true_and_needs_acknowledge_register_peer_false():
    test_setup = create_test_setup(needs_acknowledge_register_peer=False)
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.abort_timeout_sender.received_synchronize_connection()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(Timeout()))
            ]
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_acknowledge_connection_and_is_time_false(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.abort_timeout_sender.received_acknowledge_connection()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_try_send_after_acknowledge_connection_and_is_time_true_and_needs_acknowledge_register_peer_true():
    test_setup = create_test_setup(needs_acknowledge_register_peer=True)
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.abort_timeout_sender.received_acknowledge_connection()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_try_send_after_acknowledge_connection_and_is_time_true_and_needs_acknowledge_register_peer_false():
    test_setup = create_test_setup(needs_acknowledge_register_peer=False)
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.abort_timeout_sender.received_acknowledge_connection()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(Timeout()))
            ]
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_acknowledge_connection_and_acknowledge_register_peer_and_is_time_false(
        needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.abort_timeout_sender.received_acknowledge_connection()
    test_setup.abort_timeout_sender.received_acknowledge_register_peer()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_acknowledge_connection_and_acknowledge_register_peer_and_is_time_true(
        needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.abort_timeout_sender.received_acknowledge_connection()
    test_setup.abort_timeout_sender.received_acknowledge_register_peer()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_synchronize_connection_and_acknowledge_register_peer_and_is_time_false(
        needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.abort_timeout_sender.received_synchronize_connection()
    test_setup.abort_timeout_sender.received_acknowledge_register_peer()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_synchronize_connection_and_acknowledge_register_peer_and_is_time_true(
        needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.abort_timeout_sender.received_synchronize_connection()
    test_setup.abort_timeout_sender.received_acknowledge_register_peer()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_reset_timer(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    print(test_setup.timer_mock.mock_calls)
    test_setup.abort_timeout_sender.reset_timer()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.reset_timer()]
    )
