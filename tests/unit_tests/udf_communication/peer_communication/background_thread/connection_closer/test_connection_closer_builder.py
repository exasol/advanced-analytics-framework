import dataclasses
from typing import (
    List,
    Union,
)
from unittest.mock import (
    MagicMock,
    Mock,
    call,
    create_autospec,
)

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.abort_timeout_sender import (
    AbortTimeoutSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.close_connection_sender import (
    CloseConnectionSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer_builder import (
    ConnectionCloserBuilder,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer_factory import (
    ConnectionCloserFactory,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer_timeout_config import (
    ConnectionCloserTimeoutConfig,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_is_closed_sender import (
    ConnectionIsClosedSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.clock import Clock
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.timer import TimerFactory
from exasol.analytics.udf.communication.socket_factory.abstract import Socket
from tests.utils.mock_cast import mock_cast


@dataclasses.dataclass()
class TestSetup:
    __test__ = False
    peer: Peer
    my_connection_info: ConnectionInfo
    out_control_socket_mock: Union[MagicMock, Socket]
    clock_mock: Union[MagicMock, Clock]
    timeout_config: ConnectionCloserTimeoutConfig
    abort_timeout_sender_factory_mock: Union[MagicMock, AbortTimeoutSenderFactory]
    connection_is_closed_sender_factory_mock: Union[
        MagicMock, ConnectionIsClosedSenderFactory
    ]
    close_connection_sender_factory_mock: Union[MagicMock, CloseConnectionSenderFactory]
    timer_factory_mock: Union[MagicMock, TimerFactory]
    timer_mocks: list[Mock]
    sender_mock: Union[MagicMock, Sender]
    connection_closer_builder: ConnectionCloserBuilder
    connection_closer_factory_mock: Union[MagicMock, ConnectionCloserFactory]

    def reset_mock(self):
        self.connection_is_closed_sender_factory_mock.reset_mock()
        self.out_control_socket_mock.reset_mock()
        self.clock_mock.reset_mock()
        self.abort_timeout_sender_factory_mock.reset_mock()
        self.close_connection_sender_factory_mock.reset_mock()
        self.timer_factory_mock.reset_mock()
        self.sender_mock.reset_mock()
        self.connection_closer_factory_mock.reset_mock()


def create_test_setup() -> TestSetup:
    peer = Peer(
        connection_info=ConnectionInfo(
            name="t1",
            ipaddress=IPAddress(ip_address="127.0.0.1"),
            port=Port(port=11),
            group_identifier="g",
        )
    )
    my_connection_info = ConnectionInfo(
        name="t0",
        ipaddress=IPAddress(ip_address="127.0.0.1"),
        port=Port(port=10),
        group_identifier="g",
    )
    abort_timeout_sender_factory_mock: Union[MagicMock, AbortTimeoutSenderFactory] = (
        create_autospec(AbortTimeoutSenderFactory)
    )
    conncection_is_ready_sender_factory_mock: Union[
        MagicMock, ConnectionIsClosedSenderFactory
    ] = create_autospec(ConnectionIsClosedSenderFactory)
    close_connection_sender_factory_mock: Union[
        MagicMock, CloseConnectionSenderFactory
    ] = create_autospec(CloseConnectionSenderFactory)
    timer_factory_mock: Union[MagicMock, TimerFactory] = create_autospec(TimerFactory)
    timer_mocks = [Mock(), Mock(), Mock(), Mock(), Mock()]
    mock_cast(timer_factory_mock.create).side_effect = timer_mocks
    out_control_socket_mock: Union[MagicMock, Socket] = create_autospec(Socket)
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    sender_mock: Union[MagicMock, Sender] = create_autospec(Sender)
    timeout_config = ConnectionCloserTimeoutConfig(
        abort_timeout_in_ms=1,
        close_retry_timeout_in_ms=2,
        connection_is_closed_wait_time_in_ms=3,
    )
    connection_closer_factory_mock: Union[MagicMock, ConnectionCloserFactory] = (
        create_autospec(ConnectionCloserFactory)
    )
    connection_closer_builder = ConnectionCloserBuilder(
        abort_timeout_sender_factory=abort_timeout_sender_factory_mock,
        connection_is_closed_sender_factory=conncection_is_ready_sender_factory_mock,
        close_connection_sender_factory=close_connection_sender_factory_mock,
        timer_factory=timer_factory_mock,
        connection_closer_factory=connection_closer_factory_mock,
    )
    return TestSetup(
        connection_closer_builder=connection_closer_builder,
        connection_closer_factory_mock=connection_closer_factory_mock,
        peer=peer,
        my_connection_info=my_connection_info,
        out_control_socket_mock=out_control_socket_mock,
        clock_mock=clock_mock,
        abort_timeout_sender_factory_mock=abort_timeout_sender_factory_mock,
        connection_is_closed_sender_factory_mock=conncection_is_ready_sender_factory_mock,
        close_connection_sender_factory_mock=close_connection_sender_factory_mock,
        timer_factory_mock=timer_factory_mock,
        timer_mocks=timer_mocks,
        sender_mock=sender_mock,
        timeout_config=timeout_config,
    )


def test_init():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_factory_mock.create).assert_not_called()
    mock_cast(test_setup.abort_timeout_sender_factory_mock.create).assert_not_called()
    mock_cast(
        test_setup.close_connection_sender_factory_mock.create
    ).assert_not_called()
    mock_cast(
        test_setup.connection_is_closed_sender_factory_mock.create
    ).assert_not_called()
    mock_cast(test_setup.connection_closer_factory_mock.create).assert_not_called()


def test_create():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    test_setup.connection_closer_builder.create(
        my_connection_info=test_setup.my_connection_info,
        sender=test_setup.sender_mock,
        clock=test_setup.clock_mock,
        out_control_socket=test_setup.out_control_socket_mock,
        peer=test_setup.peer,
        timeout_config=test_setup.timeout_config,
    )
    assert_timer_factory(test_setup)
    test_setup.sender_mock.assert_not_called()
    test_setup.clock_mock.assert_not_called()
    test_setup.out_control_socket_mock.assert_not_called()
    assert_timer_mocks(test_setup)
    assert_close_connection_sender_factory_mock(test_setup)
    assert_abort_timeout_sender_factory_mock(test_setup)
    assert_connection_is_closed_sender_factory_mock(test_setup)


def assert_connection_is_closed_sender_factory_mock(test_setup):
    mock_cast(
        test_setup.connection_is_closed_sender_factory_mock.create
    ).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        out_control_socket=test_setup.out_control_socket_mock,
        timer=test_setup.timer_mocks[2],
    )


def assert_abort_timeout_sender_factory_mock(test_setup):
    mock_cast(
        test_setup.abort_timeout_sender_factory_mock.create
    ).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        out_control_socket=test_setup.out_control_socket_mock,
        timer=test_setup.timer_mocks[1],
        reason="Timeout occurred during establishing connection.",
    )


def assert_close_connection_sender_factory_mock(test_setup):
    mock_cast(
        test_setup.close_connection_sender_factory_mock.create
    ).assert_called_once_with(
        my_connection_info=test_setup.my_connection_info,
        peer=test_setup.peer,
        sender=test_setup.sender_mock,
        timer=test_setup.timer_mocks[0],
    )


def assert_timer_mocks(test_setup):
    for timer_mock in test_setup.timer_mocks:
        timer_mock.assert_not_called()


def assert_timer_factory(test_setup):
    test_setup.timer_factory_mock.assert_has_calls(
        [
            call.create(
                clock=test_setup.clock_mock,
                timeout_in_ms=test_setup.timeout_config.close_retry_timeout_in_ms,
            ),
            call.create(
                clock=test_setup.clock_mock,
                timeout_in_ms=test_setup.timeout_config.abort_timeout_in_ms,
            ),
            call.create(
                clock=test_setup.clock_mock,
                timeout_in_ms=test_setup.timeout_config.connection_is_closed_wait_time_in_ms,
            ),
        ]
    )
