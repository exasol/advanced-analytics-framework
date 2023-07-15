import dataclasses
from typing import Union, cast, Any, List
from unittest.mock import MagicMock, Mock, create_autospec, call

import pytest

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState, BackgroundPeerStateFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import SenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import TimerFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket, \
    SocketFactory


def mock_cast(obj: Any) -> Mock:
    return cast(Mock, obj)


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    my_connection_info: ConnectionInfo
    socket_factory_mock: Union[MagicMock, SocketFactory]
    out_control_socket_mock: Union[MagicMock, Socket]
    clock_mock: Union[MagicMock, Clock]
    acknowledge_register_peer: bool
    forward_register_peer: bool
    needs_register_peer_complete: bool
    register_peer_connection_predecessor_is_none: bool
    abort_timeout_in_ms: int
    synchronize_timeout_in_ms: int
    peer_is_ready_wait_time_in_ms: int
    send_socket_linger_time_in_ms: int
    sender_factory_mock: Union[MagicMock, SenderFactory]
    abort_timeout_sender_factory_mock: Union[MagicMock, AbortTimeoutSenderFactory]
    peer_is_ready_sender_factory_mock: Union[MagicMock, PeerIsReadySenderFactory]
    synchronize_connection_sender_factory_mock: Union[MagicMock, SynchronizeConnectionSenderFactory]
    register_peer_sender_factory_mock: Union[MagicMock, RegisterPeerSenderFactory]
    register_peer_connection_mock: Union[MagicMock, RegisterPeerConnection]
    acknowledge_register_peer_sender_factory_mock: Union[MagicMock, AcknowledgeRegisterPeerSenderFactory]
    timer_factory_mock: Union[MagicMock, TimerFactory]
    timer_mocks: List[Mock]
    background_peer_state_factory: Union[MagicMock, BackgroundPeerStateFactory]
    background_peer_state: BackgroundPeerState

    def reset_mock(self):
        self.peer_is_ready_sender_factory_mock.reset_mock()
        self.socket_factory_mock.reset_mock()
        self.register_peer_connection_mock.reset_mock()
        self.out_control_socket_mock.reset_mock()
        self.clock_mock.reset_mock()
        self.abort_timeout_sender_factory_mock.reset_mock()
        self.synchronize_connection_sender_factory_mock.reset_mock()
        self.sender_factory_mock.reset_mock()
        self.register_peer_sender_factory_mock.reset_mock()
        self.acknowledge_register_peer_sender_factory_mock.reset_mock()
        self.timer_factory_mock.reset_mock()
        self.background_peer_state_factory.reset_mock()


def create_test_setup(acknowledge_register_peer: bool,
                      forward_register_peer: bool,
                      needs_register_peer_complete: bool,
                      register_peer_connection_predecessor_is_none: bool) -> TestSetup:
    peer = Peer(
        connection_info=ConnectionInfo(
            name="t1",
            ipaddress=IPAddress(ip_address="127.0.0.1"),
            port=Port(port=11),
            group_identifier="g"
        ))
    my_connection_info = ConnectionInfo(
        name="t0",
        ipaddress=IPAddress(ip_address="127.0.0.1"),
        port=Port(port=10),
        group_identifier="g"
    )
    socket_factory_mock: Union[MagicMock, SocketFactory] = create_autospec(SocketFactory)
    sender_factory_mock: Union[MagicMock, SenderFactory] = create_autospec(SenderFactory)
    abort_timeout_sender_factory_mock: Union[MagicMock, AbortTimeoutSenderFactory] = create_autospec(
        AbortTimeoutSenderFactory)
    peer_is_ready_sender_factory_mock: Union[MagicMock, PeerIsReadySenderFactory] = create_autospec(
        PeerIsReadySenderFactory)
    synchronize_connection_sender_factory_mock: Union[MagicMock, SynchronizeConnectionSenderFactory] = create_autospec(
        SynchronizeConnectionSenderFactory)
    register_peer_sender_factory_mock: Union[MagicMock, RegisterPeerSenderFactory] = create_autospec(
        RegisterPeerSenderFactory)
    register_peer_connection_mock: Union[MagicMock, RegisterPeerConnection] = \
        create_autospec(RegisterPeerConnection, spec_set=True)
    if register_peer_connection_predecessor_is_none:
        register_peer_connection_mock.predecessor = None
    acknowledge_register_peer_sender_factory_mock: Union[
        MagicMock, AcknowledgeRegisterPeerSenderFactory] = create_autospec(AcknowledgeRegisterPeerSenderFactory)
    timer_factory_mock: Union[MagicMock, TimerFactory] = create_autospec(TimerFactory)
    timer_mocks = [Mock(), Mock(), Mock(), Mock(), Mock()]
    mock_cast(timer_factory_mock.create).side_effect = timer_mocks
    background_peer_state_factory_mock: Union[MagicMock, BackgroundPeerStateFactory] = create_autospec(
        BackgroundPeerStateFactory)
    out_control_socket_mock: Union[MagicMock, Socket] = create_autospec(Socket)
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    abort_timeout_in_ms = 1
    synchronize_timeout_in_ms = 2
    peer_is_ready_wait_time_in_ms = 3
    send_socket_linger_time_in_ms = 4
    background_peer_state = BackgroundPeerState.create(
        my_connection_info=my_connection_info,
        peer=peer,
        socket_factory=socket_factory_mock,
        acknowledge_register_peer=acknowledge_register_peer,
        forward_register_peer=forward_register_peer,
        needs_register_peer_complete=needs_register_peer_complete,
        out_control_socket=out_control_socket_mock,
        clock=clock_mock,
        abort_timeout_in_ms=abort_timeout_in_ms,
        synchronize_timeout_in_ms=synchronize_timeout_in_ms,
        peer_is_ready_wait_time_in_ms=peer_is_ready_wait_time_in_ms,
        send_socket_linger_time_in_ms=send_socket_linger_time_in_ms,
        sender_factory=sender_factory_mock,
        abort_timeout_sender_factory=abort_timeout_sender_factory_mock,
        peer_is_ready_sender_factory=peer_is_ready_sender_factory_mock,
        synchronize_connection_sender_factory=synchronize_connection_sender_factory_mock,
        register_peer_sender_factory=register_peer_sender_factory_mock,
        register_peer_connection=register_peer_connection_mock,
        acknowledge_register_peer_sender_factory=acknowledge_register_peer_sender_factory_mock,
        timer_factory=timer_factory_mock,
        background_peer_state_factory=background_peer_state_factory_mock
    )
    return TestSetup(
        background_peer_state=background_peer_state,
        peer=peer,
        my_connection_info=my_connection_info,
        socket_factory_mock=socket_factory_mock,
        out_control_socket_mock=out_control_socket_mock,
        clock_mock=clock_mock,
        acknowledge_register_peer=acknowledge_register_peer,
        forward_register_peer=forward_register_peer,
        needs_register_peer_complete=needs_register_peer_complete,
        register_peer_connection_predecessor_is_none=register_peer_connection_predecessor_is_none,
        abort_timeout_in_ms=abort_timeout_in_ms,
        synchronize_timeout_in_ms=synchronize_timeout_in_ms,
        peer_is_ready_wait_time_in_ms=peer_is_ready_wait_time_in_ms,
        send_socket_linger_time_in_ms=send_socket_linger_time_in_ms,
        sender_factory_mock=sender_factory_mock,
        abort_timeout_sender_factory_mock=abort_timeout_sender_factory_mock,
        peer_is_ready_sender_factory_mock=peer_is_ready_sender_factory_mock,
        synchronize_connection_sender_factory_mock=synchronize_connection_sender_factory_mock,
        register_peer_sender_factory_mock=register_peer_sender_factory_mock,
        register_peer_connection_mock=register_peer_connection_mock,
        acknowledge_register_peer_sender_factory_mock=acknowledge_register_peer_sender_factory_mock,
        timer_factory_mock=timer_factory_mock,
        timer_mocks=timer_mocks,
        background_peer_state_factory=background_peer_state_factory_mock
    )


@pytest.mark.parametrize(
    "acknowledge_register_peer,forward_register_peer,needs_register_peer_complete,register_peer_connection_predecessor_is_none",
    [
        (True, True, True, True),
        (True, True, True, False),
        (True, True, False, True),
        (True, True, False, False),
        (True, False, True, True),
        (True, False, True, False),
        (True, False, False, True),
        (True, False, False, False),
        (False, True, True, True),
        (False, True, True, False),
        (False, True, False, True),
        (False, True, False, False),
        (False, False, True, True),
        (False, False, True, False),
        (False, False, False, True),
        (False, False, False, False),
    ])
def test(
        acknowledge_register_peer: bool,
        forward_register_peer: bool,
        needs_register_peer_complete: bool,
        register_peer_connection_predecessor_is_none: bool
):
    test_setup = create_test_setup(
        acknowledge_register_peer=acknowledge_register_peer,
        forward_register_peer=forward_register_peer,
        needs_register_peer_complete=needs_register_peer_complete,
        register_peer_connection_predecessor_is_none=register_peer_connection_predecessor_is_none)
    assert_timer_factory(test_setup)
    assert_sender_factory(test_setup)
    test_setup.socket_factory_mock.assert_not_called()
    test_setup.clock_mock.assert_not_called()
    test_setup.out_control_socket_mock.assert_not_called()
    test_setup.register_peer_connection_mock.assert_not_called()
    assert_timer_mocks(test_setup)
    assert_synchronize_connection_sender_factory_mock(test_setup)
    assert_abort_timeout_sender_factory_mock(test_setup)
    assert_peer_is_ready_sender_factory_mock(test_setup)
    assert_register_peer_sender_factory_mock(test_setup)
    assert_acknowledge_register_peer_sender_factory_mock(test_setup)
    assert_background_peer_state_factory(test_setup)


def assert_background_peer_state_factory(test_setup):
    mock_cast(test_setup.background_peer_state_factory.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        socket_factory=test_setup.socket_factory_mock,
        peer=test_setup.peer,
        forward_register_peer=test_setup.forward_register_peer,
        acknowledge_register_peer=test_setup.acknowledge_register_peer,
        needs_register_peer_complete=test_setup.needs_register_peer_complete,
        register_peer_connection=test_setup.register_peer_connection_mock,
        sender=mock_cast(test_setup.sender_factory_mock.create).return_value,
        synchronize_connection_sender=mock_cast(
            test_setup.synchronize_connection_sender_factory_mock.create).return_value,
        abort_timeout_sender=mock_cast(test_setup.abort_timeout_sender_factory_mock.create).return_value,
        peer_is_ready_sender=mock_cast(test_setup.peer_is_ready_sender_factory_mock.create).return_value,
        register_peer_sender=mock_cast(test_setup.register_peer_sender_factory_mock.create).return_value,
        acknowledge_register_peer_sender=mock_cast(
            test_setup.acknowledge_register_peer_sender_factory_mock.create).return_value
    )


def assert_acknowledge_register_peer_sender_factory_mock(test_setup):
    mock_cast(test_setup.acknowledge_register_peer_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        register_peer_connection=test_setup.register_peer_connection_mock,
        needs_to_send_for_peer=test_setup.acknowledge_register_peer,
        timer=test_setup.timer_mocks[4]
    )


def assert_register_peer_sender_factory_mock(test_setup):
    mock_cast(test_setup.register_peer_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        register_peer_connection=test_setup.register_peer_connection_mock,
        needs_to_send_for_peer=test_setup.forward_register_peer,
        timer=test_setup.timer_mocks[3]
    )


def assert_peer_is_ready_sender_factory_mock(test_setup):
    mock_cast(test_setup.peer_is_ready_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        out_control_socket=test_setup.out_control_socket_mock,
        timer=test_setup.timer_mocks[2],
        needs_acknowledge_register_peer=test_setup.forward_register_peer,
        needs_register_peer_complete=(test_setup.needs_register_peer_complete
                                      and not test_setup.register_peer_connection_predecessor_is_none)
    )


def assert_abort_timeout_sender_factory_mock(test_setup):
    mock_cast(test_setup.abort_timeout_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        out_control_socket=test_setup.out_control_socket_mock,
        timer=test_setup.timer_mocks[1],
        needs_acknowledge_register_peer=test_setup.forward_register_peer
    )


def assert_synchronize_connection_sender_factory_mock(test_setup):
    mock_cast(test_setup.synchronize_connection_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        sender=mock_cast(test_setup.sender_factory_mock.create).return_value,
        timer=test_setup.timer_mocks[0]
    )


def assert_timer_mocks(test_setup):
    for timer_mock in test_setup.timer_mocks:
        timer_mock.assert_not_called()


def assert_sender_factory(test_setup):
    mock_cast(test_setup.sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        socket_factory=test_setup.socket_factory_mock,
        peer=test_setup.peer,
        send_socket_linger_time_in_ms=test_setup.send_socket_linger_time_in_ms
    )


def assert_timer_factory(test_setup):
    test_setup.timer_factory_mock.assert_has_calls([
        call.create(clock=test_setup.clock_mock, timeout_in_ms=test_setup.synchronize_timeout_in_ms),
        call.create(clock=test_setup.clock_mock, timeout_in_ms=test_setup.abort_timeout_in_ms),
        call.create(clock=test_setup.clock_mock, timeout_in_ms=test_setup.peer_is_ready_wait_time_in_ms),
        call.create(clock=test_setup.clock_mock, timeout_in_ms=test_setup.synchronize_timeout_in_ms),
        call.create(clock=test_setup.clock_mock, timeout_in_ms=test_setup.synchronize_timeout_in_ms),
    ])
