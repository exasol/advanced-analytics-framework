import dataclasses
from typing import Union, cast, Any
from unittest.mock import MagicMock, Mock, create_autospec, call

import pytest

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer


def mock_cast(obj: Any) -> Mock:
    return cast(Mock, obj)


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    timer_mock: Union[MagicMock, Timer]
    register_peer_connection: Union[MagicMock, RegisterPeerConnection]
    register_peer_sender: RegisterPeerSender = None

    def reset_mock(self):
        self.register_peer_connection.reset_mock()
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
    register_peer_connection = create_autospec(RegisterPeerConnection)
    register_peer_sender = RegisterPeerSender(
        peer=peer,
        my_connection_info=my_connection_info,
        register_peer_connection=register_peer_connection,
        timer=timer_mock,
    )
    return TestSetup(
        peer=peer,
        timer_mock=timer_mock,
        register_peer_connection=register_peer_connection,
        register_peer_sender=register_peer_sender
    )


def test_init():
    test_setup = create_test_setup()
    assert (
            test_setup.register_peer_connection.mock_calls == []
            and test_setup.timer_mock.mock_calls == []
    )


def test_try_send_after_init_and_is_time_false():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mock()

    test_setup.register_peer_sender.try_send()

    assert (
            test_setup.register_peer_connection.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


def test_try_send_after_init_and_is_time_true():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.register_peer_sender.try_send()
    assert (
            test_setup.register_peer_connection.mock_calls ==
            [
                call.forward(test_setup.peer)
            ]
            and test_setup.timer_mock.mock_calls == [
                call.is_time(),
                call.reset_timer()
            ]
    )

def test_try_send_after_init_twice_and_is_time_true():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.register_peer_sender.try_send()
    test_setup.reset_mock()

    test_setup.register_peer_sender.try_send()
    assert (
            test_setup.register_peer_connection.mock_calls ==
            [
                call.forward(test_setup.peer)
            ]
            and test_setup.timer_mock.mock_calls == [
                call.is_time(),
                call.reset_timer()
            ]
    )


@pytest.mark.parametrize("is_time", [True, False])
def test_try_send_after_stop_and_is_time_false(is_time: bool):
    test_setup = create_test_setup()
    test_setup.register_peer_sender.stop()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.register_peer_sender.try_send()

    assert (
            test_setup.register_peer_connection.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )
