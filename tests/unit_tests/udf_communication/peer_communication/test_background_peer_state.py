import dataclasses
from typing import Union
from unittest.mock import MagicMock, create_autospec, call

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_handler import PayloadHandler
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder import \
    RegisterPeerForwarder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Frame
from tests.mock_cast import mock_cast


@dataclasses.dataclass()
class TestSetup:
    peer: Peer
    my_connection_info: ConnectionInfo
    payload_handler_mock: Union[MagicMock, PayloadHandler]
    sender_mock: Union[MagicMock, Sender]
    connection_establisher_mock: Union[MagicMock, ConnectionEstablisher]
    register_peer_forwarder_mock: Union[MagicMock, RegisterPeerForwarder]
    background_peer_state: BackgroundPeerState

    def reset_mocks(self):
        self.sender_mock.reset_mock()
        self.payload_handler_mock.reset_mock()
        self.connection_establisher_mock.reset_mock()
        self.register_peer_forwarder_mock.reset_mock()


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
    payload_handler_mock: Union[MagicMock, PayloadHandler] = create_autospec(PayloadHandler)
    sender_mock: Union[MagicMock, Sender] = create_autospec(Sender)
    connection_establisher_mock: Union[MagicMock, ConnectionEstablisher] = create_autospec(ConnectionEstablisher)
    register_peer_forwarder_mock: Union[MagicMock, RegisterPeerForwarder] = create_autospec(RegisterPeerForwarder)
    background_peer_state = BackgroundPeerState(
        my_connection_info=my_connection_info,
        peer=peer,
        sender=sender_mock,
        connection_establisher=connection_establisher_mock,
        register_peer_forwarder=register_peer_forwarder_mock,
        payload_handler=payload_handler_mock
    )
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        payload_handler_mock=payload_handler_mock,
        sender_mock=sender_mock,
        background_peer_state=background_peer_state,
        connection_establisher_mock=connection_establisher_mock,
        register_peer_forwarder_mock=register_peer_forwarder_mock
    )


def test_init():
    test_setup = create_test_setup()
    assert (
            test_setup.sender_mock.mock_calls == []
            and test_setup.connection_establisher_mock.mock_calls == []
            and test_setup.register_peer_forwarder_mock.mock_calls == []
            and test_setup.payload_handler_mock.mock_calls == []
    )


def test_resend_if_necessary():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.try_send()
    assert (
            test_setup.connection_establisher_mock.mock_calls == [call.try_send()]
            and test_setup.register_peer_forwarder_mock.mock_calls == [call.try_send()]
            and test_setup.sender_mock.mock_calls == []
            and test_setup.payload_handler_mock.mock_calls == []
    )


def test_received_synchronize_connection():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_synchronize_connection()
    assert (
            test_setup.connection_establisher_mock.mock_calls == [call.received_synchronize_connection()]
            and test_setup.register_peer_forwarder_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and test_setup.payload_handler_mock.mock_calls == []
    )


def test_received_acknowledge_connection():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_acknowledge_connection()
    assert (
            test_setup.connection_establisher_mock.mock_calls == [call.received_acknowledge_connection()]
            and test_setup.register_peer_forwarder_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and test_setup.payload_handler_mock.mock_calls == []
    )


def test_received_acknowledge_register_peer():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_acknowledge_register_peer()
    assert (
            test_setup.register_peer_forwarder_mock.mock_calls == [call.received_acknowledge_register_peer()]
            and test_setup.connection_establisher_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and test_setup.payload_handler_mock.mock_calls == []
    )


def test_received_register_peer_complete():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.received_register_peer_complete()
    assert (
            test_setup.register_peer_forwarder_mock.mock_calls == [call.received_register_peer_complete()]
            and test_setup.connection_establisher_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and test_setup.payload_handler_mock.mock_calls == []
    )


def test_close():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    test_setup.background_peer_state.stop()
    assert (
            test_setup.connection_establisher_mock.mock_calls == []
            and test_setup.register_peer_forwarder_mock.mock_calls == []
            and test_setup.sender_mock.mock_calls == []
            and test_setup.payload_handler_mock.mock_calls == []
    )


def test_send_payload():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    frames = [create_autospec(Frame)]
    test_setup.background_peer_state.send_payload(frames=frames)
    assert mock_cast(test_setup.sender_mock.send_multipart).mock_calls == [call(frames)]
