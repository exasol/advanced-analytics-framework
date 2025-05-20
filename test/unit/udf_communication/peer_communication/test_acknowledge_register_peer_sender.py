import dataclasses
from typing import Union
from unittest.mock import (
    MagicMock,
    call,
    create_autospec,
)

import pytest

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.acknowledge_register_peer_sender import (
    AcknowledgeRegisterPeerSender,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_connection import (
    RegisterPeerConnection,
)
from exasol.analytics.udf.communication.peer_communicator.timer import Timer
from test.utils.mock_cast import mock_cast


@dataclasses.dataclass
class TestSetup:
    __test__ = False
    my_connection_info: ConnectionInfo
    peer: Peer
    register_peer_connection_mock: Union[RegisterPeerConnection, MagicMock]
    timer_mock: Union[Timer, MagicMock]
    acknowledge_register_peer_sender: AcknowledgeRegisterPeerSender

    def reset_mock(self):
        self.register_peer_connection_mock.reset_mock()
        self.timer_mock.reset_mock()


def create_test_setup(needs_to_send_for_peer: bool) -> TestSetup:
    register_peer_connection_mock: Union[RegisterPeerConnection, MagicMock] = (
        create_autospec(RegisterPeerConnection)
    )
    my_connection_info = ConnectionInfo(
        name="t0",
        port=Port(port=1),
        ipaddress=IPAddress(ip_address="127.0.0.1"),
        group_identifier="g",
    )
    peer = Peer(
        connection_info=ConnectionInfo(
            name="t1",
            port=Port(port=2),
            ipaddress=IPAddress(ip_address="127.0.0.1"),
            group_identifier="g",
        )
    )
    timer_mock: Union[Timer, MagicMock] = create_autospec(Timer)
    acknowledge_register_peer_sender = AcknowledgeRegisterPeerSender(
        register_peer_connection=register_peer_connection_mock,
        my_connection_info=my_connection_info,
        timer=timer_mock,
        peer=peer,
        needs_to_send_for_peer=needs_to_send_for_peer,
    )
    test_setup = TestSetup(
        my_connection_info=my_connection_info,
        peer=peer,
        acknowledge_register_peer_sender=acknowledge_register_peer_sender,
        timer_mock=timer_mock,
        register_peer_connection_mock=register_peer_connection_mock,
    )
    return test_setup


@pytest.mark.parametrize("needs_to_send_for_peer", [True, False])
def test_init(needs_to_send_for_peer: bool):
    test_setup = create_test_setup(needs_to_send_for_peer)
    assert (
        test_setup.register_peer_connection_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == []
    )


@pytest.mark.parametrize(
    "needs_to_send_for_peer, is_time, send_expected",
    [
        (True, True, True),
        (True, False, False),
        (False, True, False),
        (False, False, False),
    ],
)
def test_try_send_after_init(
    needs_to_send_for_peer: bool, is_time: bool, send_expected: bool
):
    test_setup = create_test_setup(needs_to_send_for_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()
    test_setup.acknowledge_register_peer_sender.try_send()
    if send_expected:
        assert test_setup.register_peer_connection_mock.mock_calls == [
            call.ack(test_setup.peer)
        ] and test_setup.timer_mock.mock_calls == [call.is_time(), call.reset_timer()]
    else:
        assert (
            test_setup.register_peer_connection_mock.mock_calls == []
            and test_setup.timer_mock.mock_calls == [call.is_time()]
        )


@pytest.mark.parametrize(
    ["needs_to_send_for_peer", "is_time"],
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_stop(needs_to_send_for_peer: bool, is_time: bool):
    test_setup = create_test_setup(needs_to_send_for_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()
    test_setup.acknowledge_register_peer_sender.stop()
    assert (
        test_setup.register_peer_connection_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == []
    )


@pytest.mark.parametrize(
    ["needs_to_send_for_peer", "is_time"],
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ],
)
def test_try_send_after_stop(needs_to_send_for_peer: bool, is_time: bool):
    test_setup = create_test_setup(needs_to_send_for_peer)
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.acknowledge_register_peer_sender.stop()
    test_setup.reset_mock()
    test_setup.acknowledge_register_peer_sender.try_send()
    assert (
        test_setup.register_peer_connection_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize("needs_to_send_for_peer", [True, False])
def test_is_ready_to_stop_after_init(needs_to_send_for_peer: bool):
    test_setup = create_test_setup(needs_to_send_for_peer)
    result = test_setup.acknowledge_register_peer_sender.is_ready_to_stop()
    assert (
        test_setup.register_peer_connection_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == []
        and result != needs_to_send_for_peer
    )


@pytest.mark.parametrize("needs_to_send_for_peer", [True, False])
def test_is_ready_to_stop_after_stop(needs_to_send_for_peer: bool):
    test_setup = create_test_setup(needs_to_send_for_peer)
    test_setup.acknowledge_register_peer_sender.stop()
    test_setup.reset_mock()
    result = test_setup.acknowledge_register_peer_sender.is_ready_to_stop()
    assert (
        test_setup.register_peer_connection_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == []
        and result == True
    )
