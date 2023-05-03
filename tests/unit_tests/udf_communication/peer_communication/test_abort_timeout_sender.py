import dataclasses
from typing import Union, cast, Any
from unittest.mock import MagicMock, Mock, create_autospec, call

import pytest

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import TimeoutMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket
from tests.mock_cast import mock_cast


@dataclasses.dataclass()
class TestSetup:
    reason: str
    timer_mock: Union[MagicMock, Timer]
    out_control_socket_mock: Union[MagicMock, Socket]
    abort_timeout_sender: AbortTimeoutSender = None

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
    reason = "test reason"
    abort_timeout_sender = AbortTimeoutSender(
        peer=peer,
        my_connection_info=my_connection_info,
        out_control_socket=out_control_socket_mock,
        timer=timer_mock,
        reason=reason
    )
    return TestSetup(
        reason=reason,
        timer_mock=timer_mock,
        out_control_socket_mock=out_control_socket_mock,
        abort_timeout_sender=abort_timeout_sender
    )


def test_init():
    test_setup = create_test_setup()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == []
    )


def test_try_send_after_init_and_is_time_false():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


def test_try_send_after_init_and_is_time_true():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(TimeoutMessage(reason=test_setup.reason)))
            ]
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


def test_try_send_twice_and_is_time_false():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.abort_timeout_sender.try_send()
    test_setup.reset_mock()

    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize("is_time", [True, False])
def test_try_send_after_stop(is_time: bool):
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.abort_timeout_sender.stop()
    test_setup.abort_timeout_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_reset_timer():
    test_setup = create_test_setup()
    print(test_setup.timer_mock.mock_calls)
    test_setup.abort_timeout_sender.reset_timer()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.reset_timer()]
    )
