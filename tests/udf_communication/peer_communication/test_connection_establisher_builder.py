import dataclasses
from typing import Union, List
from unittest.mock import MagicMock, Mock, create_autospec, call

import pytest

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_behavior_config import \
    ConnectionEstablisherBehaviorConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_builder import \
    ConnectionEstablisherBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_builder_parameter import \
    ConnectionEstablisherBuilderParameter
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_factory import \
    ConnectionEstablisherFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_timeout_config import \
    ConnectionEstablisherTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import TimerFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket
from tests.mock_cast import mock_cast


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    my_connection_info: ConnectionInfo
    out_control_socket_mock: Union[MagicMock, Socket]
    clock_mock: Union[MagicMock, Clock]
    parameter: ConnectionEstablisherBuilderParameter
    register_peer_connection_predecessor_is_none: bool
    abort_timeout_sender_factory_mock: Union[MagicMock, AbortTimeoutSenderFactory]
    peer_is_ready_sender_factory_mock: Union[MagicMock, PeerIsReadySenderFactory]
    synchronize_connection_sender_factory_mock: Union[MagicMock, SynchronizeConnectionSenderFactory]
    register_peer_sender_factory_mock: Union[MagicMock, RegisterPeerSenderFactory]
    register_peer_connection_mock: Union[MagicMock, RegisterPeerConnection]
    acknowledge_register_peer_sender_factory_mock: Union[MagicMock, AcknowledgeRegisterPeerSenderFactory]
    timer_factory_mock: Union[MagicMock, TimerFactory]
    timer_mocks: List[Mock]
    sender_mock: Union[MagicMock, Sender]
    connection_establisher_builder: ConnectionEstablisherBuilder
    connection_establisher_factory_mock: Union[MagicMock, ConnectionEstablisherFactory]

    def reset_mock(self):
        self.peer_is_ready_sender_factory_mock.reset_mock()
        self.register_peer_connection_mock.reset_mock()
        self.out_control_socket_mock.reset_mock()
        self.clock_mock.reset_mock()
        self.abort_timeout_sender_factory_mock.reset_mock()
        self.synchronize_connection_sender_factory_mock.reset_mock()
        self.register_peer_sender_factory_mock.reset_mock()
        self.acknowledge_register_peer_sender_factory_mock.reset_mock()
        self.timer_factory_mock.reset_mock()
        self.sender_mock.reset_mock()
        self.connection_establisher_factory_mock.reset_mock()


def create_test_setup(register_peer_connection_predecessor_is_none: bool,
                      behavior_config: ConnectionEstablisherBehaviorConfig) -> TestSetup:
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
        register_peer_connection_mock.predecssor = None
    acknowledge_register_peer_sender_factory_mock: Union[
        MagicMock, AcknowledgeRegisterPeerSenderFactory] = create_autospec(AcknowledgeRegisterPeerSenderFactory)
    timer_factory_mock: Union[MagicMock, TimerFactory] = create_autospec(TimerFactory)
    timer_mocks = [Mock(), Mock(), Mock(), Mock(), Mock()]
    mock_cast(timer_factory_mock.create).side_effect = timer_mocks
    out_control_socket_mock: Union[MagicMock, Socket] = create_autospec(Socket)
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    sender_mock: Union[MagicMock, Sender] = create_autospec(Sender)
    connection_establisher_timeout_config = ConnectionEstablisherTimeoutConfig(
        abort_timeout_in_ms=1,
        synchronize_retry_timeout_in_ms=2,
        peer_is_ready_wait_time_in_ms=3,
        acknowledge_register_peer_retry_timeout_in_ms=4,
        register_peer_retry_timeout_in_ms=5
    )
    connection_establisher_factory_mock: Union[MagicMock, ConnectionEstablisherFactory] = \
        create_autospec(ConnectionEstablisherFactory)
    connection_establisher_builder = ConnectionEstablisherBuilder(
        abort_timeout_sender_factory=abort_timeout_sender_factory_mock,
        peer_is_ready_sender_factory=peer_is_ready_sender_factory_mock,
        synchronize_connection_sender_factory=synchronize_connection_sender_factory_mock,
        register_peer_sender_factory=register_peer_sender_factory_mock,
        acknowledge_register_peer_sender_factory=acknowledge_register_peer_sender_factory_mock,
        timer_factory=timer_factory_mock,
        connection_establisher_factory=connection_establisher_factory_mock
    )
    parameter = ConnectionEstablisherBuilderParameter(
        register_peer_connection=register_peer_connection_mock,
        behavior_config=behavior_config,
        timeout_config=connection_establisher_timeout_config,
    )
    return TestSetup(
        connection_establisher_builder=connection_establisher_builder,
        connection_establisher_factory_mock=connection_establisher_factory_mock,
        peer=peer,
        my_connection_info=my_connection_info,
        out_control_socket_mock=out_control_socket_mock,
        clock_mock=clock_mock,
        register_peer_connection_predecessor_is_none=register_peer_connection_predecessor_is_none,
        abort_timeout_sender_factory_mock=abort_timeout_sender_factory_mock,
        peer_is_ready_sender_factory_mock=peer_is_ready_sender_factory_mock,
        synchronize_connection_sender_factory_mock=synchronize_connection_sender_factory_mock,
        register_peer_sender_factory_mock=register_peer_sender_factory_mock,
        register_peer_connection_mock=register_peer_connection_mock,
        acknowledge_register_peer_sender_factory_mock=acknowledge_register_peer_sender_factory_mock,
        timer_factory_mock=timer_factory_mock,
        timer_mocks=timer_mocks,
        sender_mock=sender_mock,
        parameter=parameter
    )


def test_init():
    test_setup = create_test_setup(
        behavior_config=ConnectionEstablisherBehaviorConfig(
            acknowledge_register_peer=True,
            forward_register_peer=True,
            needs_register_peer_complete=True
        ),
        register_peer_connection_predecessor_is_none=True)
    mock_cast(test_setup.timer_factory_mock.create).assert_not_called()
    mock_cast(test_setup.register_peer_sender_factory_mock.create).assert_not_called()
    mock_cast(test_setup.abort_timeout_sender_factory_mock.create).assert_not_called()
    mock_cast(test_setup.synchronize_connection_sender_factory_mock.create).assert_not_called()
    mock_cast(test_setup.acknowledge_register_peer_sender_factory_mock.create).assert_not_called()
    mock_cast(test_setup.peer_is_ready_sender_factory_mock.create).assert_not_called()
    mock_cast(test_setup.connection_establisher_factory_mock.create).assert_not_called()


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
def test_create(
        acknowledge_register_peer: bool,
        forward_register_peer: bool,
        needs_register_peer_complete: bool,
        register_peer_connection_predecessor_is_none: bool
):
    test_setup = create_test_setup(
        behavior_config=ConnectionEstablisherBehaviorConfig(
            acknowledge_register_peer=acknowledge_register_peer,
            forward_register_peer=forward_register_peer,
            needs_register_peer_complete=needs_register_peer_complete),
        register_peer_connection_predecessor_is_none=register_peer_connection_predecessor_is_none)
    test_setup.reset_mock()
    test_setup.connection_establisher_builder.create(
        my_connection_info=test_setup.my_connection_info,
        sender=test_setup.sender_mock,
        clock=test_setup.clock_mock,
        out_control_socket=test_setup.out_control_socket_mock,
        peer=test_setup.peer,
        parameter=test_setup.parameter
    )
    assert_timer_factory(test_setup)
    test_setup.sender_mock.assert_not_called()
    test_setup.clock_mock.assert_not_called()
    test_setup.out_control_socket_mock.assert_not_called()
    test_setup.register_peer_connection_mock.assert_not_called()
    assert_timer_mocks(test_setup)
    assert_synchronize_connection_sender_factory_mock(test_setup)
    assert_abort_timeout_sender_factory_mock(test_setup)
    assert_peer_is_ready_sender_factory_mock(test_setup)
    assert_register_peer_sender_factory_mock(test_setup)
    assert_acknowledge_register_peer_sender_factory_mock(test_setup)


def assert_acknowledge_register_peer_sender_factory_mock(test_setup):
    mock_cast(test_setup.acknowledge_register_peer_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        register_peer_connection=test_setup.parameter.register_peer_connection,
        needs_to_send_for_peer=test_setup.parameter.behavior_config.acknowledge_register_peer,
        timer=test_setup.timer_mocks[4]
    )


def assert_register_peer_sender_factory_mock(test_setup):
    mock_cast(test_setup.register_peer_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        register_peer_connection=test_setup.parameter.register_peer_connection,
        needs_to_send_for_peer=test_setup.parameter.behavior_config.forward_register_peer,
        timer=test_setup.timer_mocks[3]
    )


def assert_peer_is_ready_sender_factory_mock(test_setup):
    mock_cast(test_setup.peer_is_ready_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        out_control_socket=test_setup.out_control_socket_mock,
        timer=test_setup.timer_mocks[2],
        needs_acknowledge_register_peer=test_setup.parameter.behavior_config.forward_register_peer,
        needs_register_peer_complete=(test_setup.parameter.behavior_config.needs_register_peer_complete
                                      and not test_setup.register_peer_connection_predecessor_is_none)
    )


def assert_abort_timeout_sender_factory_mock(test_setup):
    mock_cast(test_setup.abort_timeout_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        out_control_socket=test_setup.out_control_socket_mock,
        timer=test_setup.timer_mocks[1],
        needs_acknowledge_register_peer=test_setup.parameter.behavior_config.forward_register_peer
    )


def assert_synchronize_connection_sender_factory_mock(test_setup):
    mock_cast(test_setup.synchronize_connection_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        sender=test_setup.sender_mock,
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
        call.create(
            clock=test_setup.clock_mock,
            timeout_in_ms=test_setup.parameter.timeout_config.synchronize_retry_timeout_in_ms),
        call.create(
            clock=test_setup.clock_mock,
            timeout_in_ms=test_setup.parameter.timeout_config.abort_timeout_in_ms),
        call.create(
            clock=test_setup.clock_mock,
            timeout_in_ms=test_setup.parameter.timeout_config.peer_is_ready_wait_time_in_ms),
        call.create(
            clock=test_setup.clock_mock,
            timeout_in_ms=test_setup.parameter.timeout_config.register_peer_retry_timeout_in_ms),
        call.create(
            clock=test_setup.clock_mock,
            timeout_in_ms=test_setup.parameter.timeout_config.acknowledge_register_peer_retry_timeout_in_ms),
    ])
