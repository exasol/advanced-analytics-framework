import dataclasses
from typing import Union
from unittest.mock import MagicMock, create_autospec, call

import pytest

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress, Port
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_behavior_config \
    import RegisterPeerForwarderBehaviorConfig
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_is_ready_sender import \
    RegisterPeerForwarderIsReadySender
from exasol.analytics.udf.communication.peer_communicator.timer import Timer
from exasol.analytics.udf.communication.serialization import serialize_message
from exasol.analytics.udf.communication.socket_factory.abstract import Socket
from tests.mock_cast import mock_cast


@dataclasses.dataclass
class TestSetup:
    __test__ = False
    my_connection_info: ConnectionInfo
    peer: Peer
    timer_mock: Union[Timer, MagicMock]
    out_control_socket_mock: Union[Socket, MagicMock]
    register_peer_forwarder_is_ready_sender: RegisterPeerForwarderIsReadySender

    def reset_mock(self):
        self.timer_mock.reset_mock()
        self.out_control_socket_mock.reset_mock()


def create_test_setup(behavior_config: RegisterPeerForwarderBehaviorConfig) -> TestSetup:
    my_connection_info = ConnectionInfo(name="t0",
                                        port=Port(port=1),
                                        ipaddress=IPAddress(ip_address="127.0.0.1"),
                                        group_identifier="g")
    peer = Peer(connection_info=
                ConnectionInfo(name="t1",
                               port=Port(port=2),
                               ipaddress=IPAddress(ip_address="127.0.0.1"),
                               group_identifier="g"))
    timer_mock: Union[Timer, MagicMock] = create_autospec(Timer)
    out_control_socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    register_peer_forwarder_is_ready_sender = RegisterPeerForwarderIsReadySender(
        peer=peer,
        my_connection_info=my_connection_info,
        behavior_config=behavior_config,
        timer=timer_mock,
        out_control_socket=out_control_socket_mock
    )
    test_setup = TestSetup(
        my_connection_info=my_connection_info,
        peer=peer,
        register_peer_forwarder_is_ready_sender=register_peer_forwarder_is_ready_sender,
        timer_mock=timer_mock,
        out_control_socket_mock=out_control_socket_mock
    )
    return test_setup


@pytest.mark.parametrize("needs_to_send_acknowledge_register_peer,needs_to_send_register_peer", [
    (True, True),
    (True, False),
    (False, True),
    (False, False)
])
def test_init(needs_to_send_acknowledge_register_peer: bool, needs_to_send_register_peer: bool):
    test_setup = create_test_setup(RegisterPeerForwarderBehaviorConfig(
        needs_to_send_register_peer=needs_to_send_register_peer,
        needs_to_send_acknowledge_register_peer=needs_to_send_acknowledge_register_peer
    ))
    assert (
            test_setup.timer_mock.mock_calls == []
            and test_setup.out_control_socket_mock.mock_calls == []
    )


@pytest.mark.parametrize(
    "needs_to_send_acknowledge_register_peer,"
    "needs_to_send_register_peer,"
    "is_time,"
    "expected_to_send",
    [
        (True, True, True, False),
        (True, True, False, False),
        (True, False, True, True),
        (True, False, False, False),
        (False, True, True, False),
        (False, True, False, False),
        (False, False, True, True),
        (False, False, False, True)
    ])
def test_try_send_after_init(needs_to_send_acknowledge_register_peer: bool,
                             needs_to_send_register_peer: bool,
                             is_time: bool,
                             expected_to_send):
    test_setup = create_test_setup(RegisterPeerForwarderBehaviorConfig(
        needs_to_send_register_peer=needs_to_send_register_peer,
        needs_to_send_acknowledge_register_peer=needs_to_send_acknowledge_register_peer
    ))
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.reset_mock()
    test_setup.register_peer_forwarder_is_ready_sender.try_send()
    if expected_to_send:
        assert (
                test_setup.timer_mock.mock_calls == [call.is_time()]
                and test_setup.out_control_socket_mock.mock_calls == [
                    call.send(serialize_message(messages.PeerRegisterForwarderIsReady(
                        peer=test_setup.peer
                    )))
                ]
        )
    else:
        assert (
                test_setup.timer_mock.mock_calls == [call.is_time()]
                and test_setup.out_control_socket_mock.mock_calls == []
        )


@pytest.mark.parametrize(
    "needs_to_send_acknowledge_register_peer,"
    "needs_to_send_register_peer,"
    "is_time,"
    "expected_to_send",
    [
        (True, True, True, True),
        (True, True, False, False),
        (True, False, True, True),
        (True, False, False, False),
        (False, True, True, True),
        (False, True, False, True),
        (False, False, True, True),
        (False, False, False, True)
    ])
def test_try_send_after_received_acknowledge_register_peer(
        needs_to_send_acknowledge_register_peer: bool,
        needs_to_send_register_peer: bool,
        is_time: bool,
        expected_to_send):
    test_setup = create_test_setup(RegisterPeerForwarderBehaviorConfig(
        needs_to_send_register_peer=needs_to_send_register_peer,
        needs_to_send_acknowledge_register_peer=needs_to_send_acknowledge_register_peer
    ))
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.register_peer_forwarder_is_ready_sender.received_acknowledge_register_peer()
    test_setup.reset_mock()
    test_setup.register_peer_forwarder_is_ready_sender.try_send()
    if expected_to_send:
        assert (
                test_setup.timer_mock.mock_calls == [call.is_time()]
                and test_setup.out_control_socket_mock.mock_calls == [
                    call.send(serialize_message(messages.PeerRegisterForwarderIsReady(
                        peer=test_setup.peer
                    )))
                ]
        )
    else:
        assert (
                test_setup.timer_mock.mock_calls == [call.is_time()]
                and test_setup.out_control_socket_mock.mock_calls == []
        )


@pytest.mark.parametrize(
    "needs_to_send_acknowledge_register_peer,"
    "needs_to_send_register_peer,"
    "is_time,"
    "expected_to_send",
    [
        (True, True, True, False),
        (True, True, False, False),
        (True, False, True, True),
        (True, False, False, True),
        (False, True, True, False),
        (False, True, False, False),
        (False, False, True, True),
        (False, False, False, True)
    ])
def test_try_send_after_received_register_peer_complete(
        needs_to_send_acknowledge_register_peer: bool,
        needs_to_send_register_peer: bool,
        is_time: bool,
        expected_to_send):
    test_setup = create_test_setup(RegisterPeerForwarderBehaviorConfig(
        needs_to_send_register_peer=needs_to_send_register_peer,
        needs_to_send_acknowledge_register_peer=needs_to_send_acknowledge_register_peer
    ))
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.register_peer_forwarder_is_ready_sender.received_register_peer_complete()
    test_setup.reset_mock()
    test_setup.register_peer_forwarder_is_ready_sender.try_send()
    if expected_to_send:
        assert (
                test_setup.timer_mock.mock_calls == [call.is_time()]
                and test_setup.out_control_socket_mock.mock_calls == [
                    call.send(serialize_message(messages.PeerRegisterForwarderIsReady(
                        peer=test_setup.peer
                    )))
                ]
        )
    else:
        assert (
                test_setup.timer_mock.mock_calls == [call.is_time()]
                and test_setup.out_control_socket_mock.mock_calls == []
        )


@pytest.mark.parametrize(
    "needs_to_send_acknowledge_register_peer,"
    "needs_to_send_register_peer,"
    "is_time,"
    "expected_to_send",
    [
        (True, True, True, True),
        (True, True, False, True),
        (True, False, True, True),
        (True, False, False, True),
        (False, True, True, True),
        (False, True, False, True),
        (False, False, True, True),
        (False, False, False, True)
    ])
def test_try_send_after_received_acknowledge_register_peer_and_received_register_peer_complete(
        needs_to_send_acknowledge_register_peer: bool,
        needs_to_send_register_peer: bool,
        is_time: bool,
        expected_to_send):
    test_setup = create_test_setup(RegisterPeerForwarderBehaviorConfig(
        needs_to_send_register_peer=needs_to_send_register_peer,
        needs_to_send_acknowledge_register_peer=needs_to_send_acknowledge_register_peer
    ))
    mock_cast(test_setup.timer_mock.is_time).return_value = is_time
    test_setup.register_peer_forwarder_is_ready_sender.received_register_peer_complete()
    test_setup.register_peer_forwarder_is_ready_sender.received_acknowledge_register_peer()
    test_setup.reset_mock()
    test_setup.register_peer_forwarder_is_ready_sender.try_send()
    if expected_to_send:
        assert (
                test_setup.timer_mock.mock_calls == [call.is_time()]
                and test_setup.out_control_socket_mock.mock_calls == [
                    call.send(serialize_message(messages.PeerRegisterForwarderIsReady(
                        peer=test_setup.peer
                    )))
                ]
        )
    else:
        assert (
                test_setup.timer_mock.mock_calls == [call.is_time()]
                and test_setup.out_control_socket_mock.mock_calls == []
        )
