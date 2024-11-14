import dataclasses
from typing import Union, cast, Any
from unittest.mock import MagicMock, Mock, create_autospec, call

import pytest

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress, Port
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol.analytics.udf.communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender
from exasol.analytics.udf.communication.peer_communicator.timer import Timer
from tests.mock_cast import mock_cast


@dataclasses.dataclass()
class TestSetup:
    __test__ = False
    peer: Peer
    timer_mock: Union[MagicMock, Timer]
    register_peer_connection: Union[MagicMock, RegisterPeerConnection]
    register_peer_sender: RegisterPeerSender = None

    def reset_mock(self):
        self.register_peer_connection.reset_mock()
        self.timer_mock.reset_mock()


def create_test_setup(needs_to_send_for_peer: bool):
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
        needs_to_send_for_peer=needs_to_send_for_peer
    )
    return TestSetup(
        peer=peer,
        timer_mock=timer_mock,
        register_peer_connection=register_peer_connection,
        register_peer_sender=register_peer_sender
    )


@pytest.mark.parametrize("needs_to_send_for_peer",
                         [
                             (True,),
                             (False,),
                         ])
def test_init(needs_to_send_for_peer: bool):
    test_setup = create_test_setup(needs_to_send_for_peer=needs_to_send_for_peer)
    assert (
            test_setup.register_peer_connection.mock_calls == []
            and test_setup.timer_mock.mock_calls == []
    )


@pytest.mark.parametrize("needs_to_send_for_peer,is_time,send_expected",
                         [
                             (True, True, True),
                             (True, False, False),
                             (False, True, False),
                             (False, False, False),

                         ])
def test_try_send_after_init(needs_to_send_for_peer: bool, is_time: bool, send_expected: bool):
    test_setup = create_test_setup(needs_to_send_for_peer=needs_to_send_for_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.register_peer_sender.try_send()

    if send_expected:
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
    else:
        assert (
                test_setup.register_peer_connection.mock_calls == []
                and test_setup.timer_mock.mock_calls == [
                    call.is_time()
                ]
        )


@pytest.mark.parametrize("needs_to_send_for_peer,is_time,send_expected",
                         [
                             (True, True, True),
                             (True, False, False),
                             (False, True, False),
                             (False, False, False),
                         ])
def test_try_send_after_init_twice(needs_to_send_for_peer: bool, is_time: bool, send_expected: bool):
    test_setup = create_test_setup(needs_to_send_for_peer=needs_to_send_for_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.register_peer_sender.try_send()
    test_setup.reset_mock()

    test_setup.register_peer_sender.try_send()
    if send_expected:
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
    else:
        assert (
                test_setup.register_peer_connection.mock_calls == []
                and test_setup.timer_mock.mock_calls == [
                    call.is_time()
                ]
        )


@pytest.mark.parametrize("needs_to_send_for_peer,is_time",
                         [
                             (True, True),
                             (True, False),
                             (False, True),
                             (False, False),
                         ])
def test_try_send_after_stop(needs_to_send_for_peer: bool, is_time: bool):
    test_setup = create_test_setup(needs_to_send_for_peer=needs_to_send_for_peer)
    test_setup.register_peer_sender.stop()
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()

    test_setup.register_peer_sender.try_send()

    assert (
            test_setup.register_peer_connection.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
    )
