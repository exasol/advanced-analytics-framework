import dataclasses
from typing import Union, cast, Any
from unittest.mock import MagicMock, Mock, create_autospec, call

import pytest

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import PeerIsReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket


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


def create_test_setup(*, needs_acknowledge_register_peer: bool):
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
        timer=timer_mock,
        needs_acknowledge_register_peer=needs_acknowledge_register_peer
    )
    return TestSetup(
        peer=peer,
        timer_mock=timer_mock,
        out_control_socket_mock=out_control_socket_mock,
        peer_is_ready_sender=peer_is_ready_sender
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_init(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(
        needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == []
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_init_and_is_time_false(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(
        needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_init_and_is_time_true(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(
        needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [
                call.is_time()
            ]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_synchronize_connection_and_is_time_false(
        needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(
        needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    test_setup.peer_is_ready_sender.received_synchronize_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_try_send_after_synchronize_connection_and_is_time_true_and_needs_acknowledge_register_peer_false():
    test_setup = create_test_setup(needs_acknowledge_register_peer=False)
    test_setup.peer_is_ready_sender.received_synchronize_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(PeerIsReadyToReceiveMessage(peer=test_setup.peer)))
            ]
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )

@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_try_send_after_synchronize_connection_and_acknowledge_register_peer_and_is_time_true(
        needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=False)
    test_setup.peer_is_ready_sender.received_synchronize_connection()
    test_setup.peer_is_ready_sender.received_acknowledge_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(PeerIsReadyToReceiveMessage(peer=test_setup.peer)))
            ]
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_try_send_twice_after_synchronize_connection_and_is_time_true_and_needs_acknowledge_register_peer_false():
    test_setup = create_test_setup(needs_acknowledge_register_peer=False)
    test_setup.peer_is_ready_sender.received_synchronize_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.peer_is_ready_sender.try_send()
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("is_time,", [True, False])
def test_try_send_after_acknowledge_connection_and_needs_acknowledge_register_peer_false(is_time: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=False)
    test_setup.peer_is_ready_sender.received_acknowledge_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(PeerIsReadyToReceiveMessage(peer=test_setup.peer)))
            ]
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("is_time", [True, False])
def test_try_send_after_acknowledge_connection_and_needs_acknowledge_register_peer_true(is_time: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=True)
    test_setup.peer_is_ready_sender.received_acknowledge_connection()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("is_time,needs_acknowledge_register_peer",
                         [
                             (True, True),
                             (True, False),
                             (False, False),
                             (False, True),
                         ])
def test_try_send_after_acknowledge_connection_and_acknowledge_register_peer(
        is_time: bool, needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=True)
    test_setup.peer_is_ready_sender.received_acknowledge_connection()
    test_setup.peer_is_ready_sender.received_acknowledge_register_peer()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.peer_is_ready_sender.try_send()

    assert (
            test_setup.out_control_socket_mock.mock_calls ==
            [
                call.send(serialize_message(PeerIsReadyToReceiveMessage(peer=test_setup.peer)))
            ]
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("needs_acknowledge_register_peer", [True, False])
def test_reset_timer(needs_acknowledge_register_peer: bool):
    test_setup = create_test_setup(needs_acknowledge_register_peer=needs_acknowledge_register_peer)
    print(test_setup.timer_mock.mock_calls)
    test_setup.peer_is_ready_sender.reset_timer()
    assert (
            test_setup.out_control_socket_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.reset_timer()]
    )
