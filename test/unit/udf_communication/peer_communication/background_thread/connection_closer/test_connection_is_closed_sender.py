import dataclasses
from typing import Union
from unittest.mock import (
    MagicMock,
    call,
    create_autospec,
)

import pytest
from test.utils.mock_cast import mock_cast

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_is_closed_sender import (
    ConnectionIsClosedSender,
)
from exasol.analytics.udf.communication.peer_communicator.timer import Timer
from exasol.analytics.udf.communication.serialization import serialize_message
from exasol.analytics.udf.communication.socket_factory.abstract import Socket


@dataclasses.dataclass()
class TestSetup:
    __test__ = False
    peer: Peer
    timer_mock: Union[MagicMock, Timer]
    out_control_socket_mock: Union[MagicMock, Socket]
    peer_is_closed_sender: ConnectionIsClosedSender = None

    def reset_mock(self):
        self.out_control_socket_mock.reset_mock()
        self.timer_mock.reset_mock()


def create_test_setup():
    peer = Peer(
        connection_info=ConnectionInfo(
            name="t2",
            ipaddress=IPAddress(ip_address="127.0.0.1"),
            port=Port(port=12),
            group_identifier="g",
        )
    )
    my_connection_info = ConnectionInfo(
        name="t1",
        ipaddress=IPAddress(ip_address="127.0.0.1"),
        port=Port(port=11),
        group_identifier="g",
    )
    timer_mock = create_autospec(Timer)
    out_control_socket_mock = create_autospec(Socket)
    connection_is_ready_sender = ConnectionIsClosedSender(
        peer=peer,
        my_connection_info=my_connection_info,
        out_control_socket=out_control_socket_mock,
        timer=timer_mock,
    )
    return TestSetup(
        peer=peer,
        timer_mock=timer_mock,
        out_control_socket_mock=out_control_socket_mock,
        peer_is_closed_sender=connection_is_ready_sender,
    )


def test_init():
    test_setup = create_test_setup()
    assert (
        test_setup.out_control_socket_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == []
    )


@pytest.mark.parametrize("is_time", [True, False])
def test_try_send_after_init(is_time: bool):
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.peer_is_closed_sender.try_send()

    assert (
        test_setup.out_control_socket_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize(
    "is_time,send_expected",
    [
        (True, True),
        (False, False),
    ],
)
def test_try_send_after_synchronize_connection(is_time: bool, send_expected: bool):
    test_setup = create_test_setup()
    test_setup.peer_is_closed_sender.received_close_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.peer_is_closed_sender.try_send()

    if send_expected:
        assert test_setup.out_control_socket_mock.mock_calls == [
            call.send(
                serialize_message(messages.ConnectionIsClosed(peer=test_setup.peer))
            )
        ] and test_setup.timer_mock.mock_calls == [call.is_time()]
    else:
        assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
        )


@pytest.mark.parametrize("is_time", [True, False])
def test_try_send_after_acknowledge_connection(is_time: bool):
    test_setup = create_test_setup()
    test_setup.peer_is_closed_sender.received_acknowledge_close_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.peer_is_closed_sender.try_send()

    assert (
        test_setup.out_control_socket_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("is_time", [True, False])
def test_try_send_after_synchronize_connection_and_acknowledge_connection(
    is_time: bool,
):
    test_setup = create_test_setup()
    test_setup.peer_is_closed_sender.received_close_connection()
    test_setup.peer_is_closed_sender.received_acknowledge_close_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.peer_is_closed_sender.try_send()

    assert test_setup.out_control_socket_mock.mock_calls == [
        call.send(serialize_message(messages.ConnectionIsClosed(peer=test_setup.peer)))
    ] and test_setup.timer_mock.mock_calls == [call.is_time()]


@pytest.mark.parametrize("is_time", [True, False])
def test_try_send_twice_after_synchronize_connection(is_time: bool):
    test_setup = create_test_setup()
    test_setup.peer_is_closed_sender.received_close_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.peer_is_closed_sender.try_send()
    test_setup.reset_mock()

    test_setup.peer_is_closed_sender.try_send()

    assert (
        test_setup.out_control_socket_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("is_time", [True, False])
def test_try_send_twice_after_acknowledge_connection(is_time: bool):
    test_setup = create_test_setup()
    test_setup.peer_is_closed_sender.received_acknowledge_close_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.peer_is_closed_sender.try_send()
    test_setup.reset_mock()

    test_setup.peer_is_closed_sender.try_send()

    assert (
        test_setup.out_control_socket_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == [call.is_time()]
    )
