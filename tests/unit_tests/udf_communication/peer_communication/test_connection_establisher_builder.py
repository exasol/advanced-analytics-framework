import dataclasses
from typing import Union, List
from unittest.mock import MagicMock, Mock, create_autospec, call

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress, Port
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSenderFactory
from exasol.analytics.udf.communication.peer_communicator.clock import Clock
from exasol.analytics.udf.communication.peer_communicator.connection_establisher_builder import \
    ConnectionEstablisherBuilder
from exasol.analytics.udf.communication.peer_communicator.connection_establisher_factory import \
    ConnectionEstablisherFactory
from exasol.analytics.udf.communication.peer_communicator.connection_establisher_timeout_config import \
    ConnectionEstablisherTimeoutConfig
from exasol.analytics.udf.communication.peer_communicator.connection_is_ready_sender import \
    ConnectionIsReadySenderFactory
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSenderFactory
from exasol.analytics.udf.communication.peer_communicator.timer import TimerFactory
from exasol.analytics.udf.communication.socket_factory.abstract import Socket
from tests.mock_cast import mock_cast


@dataclasses.dataclass()
class TestSetup:
    __test__ = False
    peer: Peer
    my_connection_info: ConnectionInfo
    out_control_socket_mock: Union[MagicMock, Socket]
    clock_mock: Union[MagicMock, Clock]
    timeout_config: ConnectionEstablisherTimeoutConfig
    abort_timeout_sender_factory_mock: Union[MagicMock, AbortTimeoutSenderFactory]
    connection_is_ready_sender_factory_mock: Union[MagicMock, ConnectionIsReadySenderFactory]
    synchronize_connection_sender_factory_mock: Union[MagicMock, SynchronizeConnectionSenderFactory]
    timer_factory_mock: Union[MagicMock, TimerFactory]
    timer_mocks: List[Mock]
    sender_mock: Union[MagicMock, Sender]
    connection_establisher_builder: ConnectionEstablisherBuilder
    connection_establisher_factory_mock: Union[MagicMock, ConnectionEstablisherFactory]

    def reset_mock(self):
        self.connection_is_ready_sender_factory_mock.reset_mock()
        self.out_control_socket_mock.reset_mock()
        self.clock_mock.reset_mock()
        self.abort_timeout_sender_factory_mock.reset_mock()
        self.synchronize_connection_sender_factory_mock.reset_mock()
        self.timer_factory_mock.reset_mock()
        self.sender_mock.reset_mock()
        self.connection_establisher_factory_mock.reset_mock()


def create_test_setup() -> TestSetup:
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
    conncection_is_ready_sender_factory_mock: Union[MagicMock, ConnectionIsReadySenderFactory] = create_autospec(
        ConnectionIsReadySenderFactory)
    synchronize_connection_sender_factory_mock: Union[MagicMock, SynchronizeConnectionSenderFactory] = create_autospec(
        SynchronizeConnectionSenderFactory)
    timer_factory_mock: Union[MagicMock, TimerFactory] = create_autospec(TimerFactory)
    timer_mocks = [Mock(), Mock(), Mock(), Mock(), Mock()]
    mock_cast(timer_factory_mock.create).side_effect = timer_mocks
    out_control_socket_mock: Union[MagicMock, Socket] = create_autospec(Socket)
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    sender_mock: Union[MagicMock, Sender] = create_autospec(Sender)
    timeout_config = ConnectionEstablisherTimeoutConfig(
        abort_timeout_in_ms=1,
        synchronize_retry_timeout_in_ms=2,
        connection_is_ready_wait_time_in_ms=3,

    )
    connection_establisher_factory_mock: Union[MagicMock, ConnectionEstablisherFactory] = \
        create_autospec(ConnectionEstablisherFactory)
    connection_establisher_builder = ConnectionEstablisherBuilder(
        abort_timeout_sender_factory=abort_timeout_sender_factory_mock,
        connection_is_ready_sender_factory=conncection_is_ready_sender_factory_mock,
        synchronize_connection_sender_factory=synchronize_connection_sender_factory_mock,
        timer_factory=timer_factory_mock,
        connection_establisher_factory=connection_establisher_factory_mock
    )
    return TestSetup(
        connection_establisher_builder=connection_establisher_builder,
        connection_establisher_factory_mock=connection_establisher_factory_mock,
        peer=peer,
        my_connection_info=my_connection_info,
        out_control_socket_mock=out_control_socket_mock,
        clock_mock=clock_mock,
        abort_timeout_sender_factory_mock=abort_timeout_sender_factory_mock,
        connection_is_ready_sender_factory_mock=conncection_is_ready_sender_factory_mock,
        synchronize_connection_sender_factory_mock=synchronize_connection_sender_factory_mock,
        timer_factory_mock=timer_factory_mock,
        timer_mocks=timer_mocks,
        sender_mock=sender_mock,
        timeout_config=timeout_config
    )


def test_init():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_factory_mock.create).assert_not_called()
    mock_cast(test_setup.abort_timeout_sender_factory_mock.create).assert_not_called()
    mock_cast(test_setup.synchronize_connection_sender_factory_mock.create).assert_not_called()
    mock_cast(test_setup.connection_is_ready_sender_factory_mock.create).assert_not_called()
    mock_cast(test_setup.connection_establisher_factory_mock.create).assert_not_called()


def test_create():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_establisher_builder.create(
        my_connection_info=test_setup.my_connection_info,
        sender=test_setup.sender_mock,
        clock=test_setup.clock_mock,
        out_control_socket=test_setup.out_control_socket_mock,
        peer=test_setup.peer,
        timeout_config=test_setup.timeout_config
    )
    assert_timer_factory(test_setup)
    test_setup.sender_mock.assert_not_called()
    test_setup.clock_mock.assert_not_called()
    test_setup.out_control_socket_mock.assert_not_called()
    assert_timer_mocks(test_setup)
    assert_synchronize_connection_sender_factory_mock(test_setup)
    assert_abort_timeout_sender_factory_mock(test_setup)
    assert_connection_is_ready_sender_factory_mock(test_setup)


def assert_connection_is_ready_sender_factory_mock(test_setup):
    mock_cast(test_setup.connection_is_ready_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        out_control_socket=test_setup.out_control_socket_mock,
        timer=test_setup.timer_mocks[2],
    )


def assert_abort_timeout_sender_factory_mock(test_setup):
    mock_cast(test_setup.abort_timeout_sender_factory_mock.create).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        out_control_socket=test_setup.out_control_socket_mock,
        timer=test_setup.timer_mocks[1],
        reason='Timeout occurred during establishing connection.'
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


def assert_timer_factory(test_setup):
    test_setup.timer_factory_mock.assert_has_calls([
        call.create(
            clock=test_setup.clock_mock,
            timeout_in_ms=test_setup.timeout_config.synchronize_retry_timeout_in_ms),
        call.create(
            clock=test_setup.clock_mock,
            timeout_in_ms=test_setup.timeout_config.abort_timeout_in_ms),
        call.create(
            clock=test_setup.clock_mock,
            timeout_in_ms=test_setup.timeout_config.connection_is_ready_wait_time_in_ms),
    ])
