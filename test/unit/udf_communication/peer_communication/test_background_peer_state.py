import dataclasses
from typing import Union
from unittest.mock import (
    MagicMock,
    call,
    create_autospec,
)

import pytest
from polyfactory.factories.pydantic_factory import ModelFactory

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.background_peer_state import (
    BackgroundPeerState,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer import (
    ConnectionCloser,
)
from exasol.analytics.udf.communication.peer_communicator.connection_establisher import (
    ConnectionEstablisher,
)
from exasol.analytics.udf.communication.peer_communicator.payload_handler import (
    PayloadHandler,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder import (
    RegisterPeerForwarder,
)
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.socket_factory.abstract import Frame
from test.utils.mock_cast import mock_cast


@dataclasses.dataclass()
class TestSetup:
    __test__ = False
    peer: Peer
    my_connection_info: ConnectionInfo
    payload_handler_mock: Union[MagicMock, PayloadHandler]
    sender_mock: Union[MagicMock, Sender]
    connection_establisher_mock: Union[MagicMock, ConnectionEstablisher]
    connection_closer_mock: Union[MagicMock, ConnectionCloser]
    register_peer_forwarder_mock: Union[MagicMock, RegisterPeerForwarder]
    background_peer_state: BackgroundPeerState

    def reset_mocks(self):
        self.sender_mock.reset_mock()
        self.payload_handler_mock.reset_mock()
        self.connection_establisher_mock.reset_mock()
        self.connection_closer_mock.reset_mock()
        self.register_peer_forwarder_mock.reset_mock()


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
    payload_handler_mock: Union[MagicMock, PayloadHandler] = create_autospec(
        PayloadHandler
    )
    sender_mock: Union[MagicMock, Sender] = create_autospec(Sender)
    connection_establisher_mock: Union[MagicMock, ConnectionEstablisher] = (
        create_autospec(ConnectionEstablisher)
    )
    connection_closer_mock: Union[MagicMock, ConnectionCloser] = create_autospec(
        ConnectionCloser
    )
    register_peer_forwarder_mock: Union[MagicMock, RegisterPeerForwarder] = (
        create_autospec(RegisterPeerForwarder)
    )
    background_peer_state = BackgroundPeerState(
        my_connection_info=my_connection_info,
        peer=peer,
        sender=sender_mock,
        connection_establisher=connection_establisher_mock,
        register_peer_forwarder=register_peer_forwarder_mock,
        payload_handler=payload_handler_mock,
        connection_closer=connection_closer_mock,
    )
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        payload_handler_mock=payload_handler_mock,
        sender_mock=sender_mock,
        background_peer_state=background_peer_state,
        connection_establisher_mock=connection_establisher_mock,
        register_peer_forwarder_mock=register_peer_forwarder_mock,
        connection_closer_mock=connection_closer_mock,
    )


def test_init():
    test_setup = create_test_setup()
    assert (
        test_setup.sender_mock.mock_calls == []
        and test_setup.connection_establisher_mock.mock_calls == []
        and test_setup.register_peer_forwarder_mock.mock_calls == []
        and test_setup.payload_handler_mock.mock_calls == []
    )


@pytest.mark.parametrize(
    "connection_establisher_ready,register_peer_forwarder_ready,payload_handler_ready",
    [
        (True, True, True),
        (True, True, False),
        (True, False, True),
        (True, False, False),
        (False, True, True),
        (False, True, False),
        (False, False, True),
        (False, False, False),
    ],
)
def test_try_send_no_prepare_close(
    connection_establisher_ready: bool,
    register_peer_forwarder_ready: bool,
    payload_handler_ready: bool,
):
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    mock_cast(test_setup.connection_establisher_mock.is_ready_to_stop).return_value = (
        connection_establisher_ready
    )
    mock_cast(test_setup.register_peer_forwarder_mock.is_ready_to_stop).return_value = (
        register_peer_forwarder_ready
    )
    mock_cast(test_setup.payload_handler_mock.is_ready_to_stop).return_value = (
        payload_handler_ready
    )
    test_setup.background_peer_state.try_send()
    assert (
        test_setup.connection_establisher_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.register_peer_forwarder_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.sender_mock.mock_calls == []
        and test_setup.payload_handler_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.connection_closer_mock.mock_calls == []
    )


@pytest.mark.parametrize(
    "connection_establisher_ready,register_peer_forwarder_ready,payload_handler_ready",
    [
        (True, True, False),
        (True, False, True),
        (True, False, False),
        (False, True, True),
        (False, True, False),
        (False, False, True),
        (False, False, False),
    ],
)
def test_try_send_prepare_close_not_ready(
    connection_establisher_ready: bool,
    register_peer_forwarder_ready: bool,
    payload_handler_ready: bool,
):
    test_setup = create_test_setup()
    test_setup.background_peer_state.prepare_to_stop()
    test_setup.reset_mocks()
    mock_cast(test_setup.connection_establisher_mock.is_ready_to_stop).return_value = (
        connection_establisher_ready
    )
    mock_cast(test_setup.register_peer_forwarder_mock.is_ready_to_stop).return_value = (
        register_peer_forwarder_ready
    )
    mock_cast(test_setup.payload_handler_mock.is_ready_to_stop).return_value = (
        payload_handler_ready
    )
    test_setup.background_peer_state.try_send()
    assert (
        test_setup.connection_establisher_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.register_peer_forwarder_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.sender_mock.mock_calls == []
        and test_setup.payload_handler_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.connection_closer_mock.mock_calls == []
    )


def test_try_send_prepare_close_ready():
    test_setup = create_test_setup()
    test_setup.background_peer_state.prepare_to_stop()
    test_setup.reset_mocks()
    mock_cast(test_setup.connection_establisher_mock.is_ready_to_stop).return_value = (
        True
    )
    mock_cast(test_setup.register_peer_forwarder_mock.is_ready_to_stop).return_value = (
        True
    )
    mock_cast(test_setup.payload_handler_mock.is_ready_to_stop).return_value = True
    test_setup.background_peer_state.try_send()
    assert (
        test_setup.connection_establisher_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.register_peer_forwarder_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.sender_mock.mock_calls == []
        and test_setup.payload_handler_mock.mock_calls
        == [call.try_send(), call.is_ready_to_stop()]
        and test_setup.connection_closer_mock.mock_calls == [call.try_send()]
    )


def test_received_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_synchronize_connection()
    assert (
        test_setup.connection_establisher_mock.mock_calls
        == [call.received_synchronize_connection()]
        and test_setup.register_peer_forwarder_mock.mock_calls == []
        and test_setup.sender_mock.mock_calls == []
        and test_setup.payload_handler_mock.mock_calls == []
    )


def test_received_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_acknowledge_connection()
    assert (
        test_setup.connection_establisher_mock.mock_calls
        == [call.received_acknowledge_connection()]
        and test_setup.register_peer_forwarder_mock.mock_calls == []
        and test_setup.sender_mock.mock_calls == []
        and test_setup.payload_handler_mock.mock_calls == []
    )


def test_received_acknowledge_register_peer():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_acknowledge_register_peer()
    assert (
        test_setup.register_peer_forwarder_mock.mock_calls
        == [call.received_acknowledge_register_peer()]
        and test_setup.connection_establisher_mock.mock_calls == []
        and test_setup.sender_mock.mock_calls == []
        and test_setup.payload_handler_mock.mock_calls == []
    )


def test_received_register_peer_complete():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_register_peer_complete()
    assert (
        test_setup.register_peer_forwarder_mock.mock_calls
        == [call.received_register_peer_complete()]
        and test_setup.connection_establisher_mock.mock_calls == []
        and test_setup.sender_mock.mock_calls == []
        and test_setup.payload_handler_mock.mock_calls == []
    )


def test_send_payload():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    frames = [create_autospec(Frame)]
    payload = ModelFactory.create_factory(model=messages.Payload).build()
    test_setup.background_peer_state.send_payload(message=payload, frames=frames)
    assert mock_cast(test_setup.payload_handler_mock.send_payload).mock_calls == [
        call(payload, frames)
    ]
