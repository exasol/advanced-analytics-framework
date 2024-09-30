import dataclasses
from typing import Union, Tuple, List
from unittest.mock import create_autospec, MagicMock, call

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender import \
    PayloadMessageSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender_factory import \
    PayloadMessageSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender_timeout_config import \
    PayloadMessageSenderTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_sender import PayloadSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket, Frame
from tests.mock_cast import mock_cast


@dataclasses.dataclass
class TestSetup:
    __test__ = False
    sender_mock: Union[MagicMock, Sender]
    out_control_socket_mock: Union[MagicMock, Socket]
    clock_mock: Union[MagicMock, Clock]
    payload_message_sender_factory_mock: Union[MagicMock, PayloadMessageSenderFactory]
    payload_message_sender_mocks: List[Union[MagicMock, PayloadMessageSender]]
    payload_message_sender_timeout_config: PayloadMessageSenderTimeoutConfig
    my_connection_info: ConnectionInfo
    peer: Peer
    payload_sender: PayloadSender

    def reset_mock(self):
        self.out_control_socket_mock.reset_mock()
        self.sender_mock.reset_mock()
        self.clock_mock.reset_mock()
        self.payload_message_sender_factory_mock.reset_mock()
        for payload_message_sender_mock in self.payload_message_sender_mocks:
            payload_message_sender_mock.reset_mock()


def create_test_setup(number_of_messages: int) -> TestSetup:
    sender_mock = create_autospec(Sender)
    out_control_socket_mock = create_autospec(Socket)
    my_connection_info = ConnectionInfo(name="t1",
                                        ipaddress=IPAddress(ip_address="127.0.0.1"),
                                        port=Port(port=1000),
                                        group_identifier="group")
    peer = Peer(connection_info=ConnectionInfo(name="t2",
                                               ipaddress=IPAddress(ip_address="127.0.0.1"),
                                               port=Port(port=2000),
                                               group_identifier="group"))
    clock_mock = create_autospec(Clock)
    payload_message_sender_factory_mock: Union[MagicMock, PayloadMessageSenderFactory] = \
        create_autospec(PayloadMessageSenderFactory)
    payload_message_sender_mocks = [create_autospec(PayloadMessageSender) for i in range(number_of_messages)]
    mock_cast(payload_message_sender_factory_mock.create).side_effect = payload_message_sender_mocks
    payload_message_sender_timeout_config = PayloadMessageSenderTimeoutConfig(
        abort_timeout_in_ms=2,
        retry_timeout_in_ms=1
    )
    payload_sender = PayloadSender(sender=sender_mock,
                                   out_control_socket=out_control_socket_mock,
                                   my_connection_info=my_connection_info,
                                   peer=peer,
                                   clock=clock_mock,
                                   payload_message_sender_factory=payload_message_sender_factory_mock,
                                   payload_message_sender_timeout_config=payload_message_sender_timeout_config)
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        sender_mock=sender_mock,
        out_control_socket_mock=out_control_socket_mock,
        clock_mock=clock_mock,
        payload_message_sender_factory_mock=payload_message_sender_factory_mock,
        payload_message_sender_mocks=payload_message_sender_mocks,
        payload_message_sender_timeout_config=payload_message_sender_timeout_config,
        payload_sender=payload_sender
    )


def create_acknowledge_payload_message(test_setup: TestSetup, message: messages.Payload) -> messages.Message:
    acknowledge_message = messages.Message(__root__=messages.AcknowledgePayload(
        source=Peer(connection_info=test_setup.my_connection_info),
        sequence_number=message.sequence_number,
        destination=test_setup.peer
    ))
    return acknowledge_message


def create_payload_message(test_setup: TestSetup, sequence_number: int) -> Tuple[messages.Payload, List[Frame]]:
    frames = [create_autospec(Frame)]
    message = messages.Payload(
        source=test_setup.peer,
        destination=Peer(connection_info=test_setup.my_connection_info),
        sequence_number=sequence_number
    )
    return message, frames


def test_init():
    test_setup = create_test_setup(number_of_messages=0)
    assert test_setup.out_control_socket_mock.mock_calls == [] \
           and test_setup.sender_mock.mock_calls == [] \
           and test_setup.clock_mock.mock_calls == [] \
           and test_setup.payload_message_sender_factory_mock.mock_calls == []


def test_try_send():
    test_setup = create_test_setup(number_of_messages=0)
    test_setup.reset_mock()
    test_setup.payload_sender.try_send()
    assert test_setup.out_control_socket_mock.mock_calls == [] \
           and test_setup.sender_mock.mock_calls == [] \
           and test_setup.clock_mock.mock_calls == [] \
           and test_setup.payload_message_sender_factory_mock.mock_calls == []


def test_send_payload():
    test_setup = create_test_setup(number_of_messages=1)
    test_setup.reset_mock()
    payload_message, frames = create_payload_message(test_setup, 0)
    test_setup.payload_sender.send_payload(payload_message, frames)
    assert test_setup.out_control_socket_mock.mock_calls == [] \
           and test_setup.sender_mock.mock_calls == [] \
           and test_setup.clock_mock.mock_calls == [] \
           and test_setup.payload_message_sender_factory_mock.mock_calls == [
               call.create(message=payload_message,
                           frames=frames,
                           sender=test_setup.sender_mock,
                           out_control_socket=test_setup.out_control_socket_mock,
                           clock=test_setup.clock_mock,
                           payload_message_sender_timeout_config=test_setup.payload_message_sender_timeout_config)] \
           and test_setup.payload_message_sender_mocks[0].mock_calls == []


def test_try_send_after_send_payload():
    test_setup = create_test_setup(number_of_messages=1)
    payload_message, frames = create_payload_message(test_setup, 0)
    test_setup.payload_sender.send_payload(payload_message, frames)
    test_setup.reset_mock()
    test_setup.payload_sender.try_send()
    assert test_setup.out_control_socket_mock.mock_calls == [] \
           and test_setup.sender_mock.mock_calls == [] \
           and test_setup.clock_mock.mock_calls == [] \
           and test_setup.payload_message_sender_factory_mock.mock_calls == [] \
           and test_setup.payload_message_sender_mocks[0].mock_calls == [call.try_send()]


def test_received_acknowledge_payload_after_send_payload():
    test_setup = create_test_setup(number_of_messages=1)
    payload_message, frames = create_payload_message(test_setup, 0)
    test_setup.payload_sender.send_payload(payload_message, frames)
    test_setup.reset_mock()
    acknowledge_payload_message = create_acknowledge_payload_message(test_setup, payload_message)
    test_setup.payload_sender.received_acknowledge_payload(message=acknowledge_payload_message.__root__)
    assert test_setup.out_control_socket_mock.mock_calls == \
           [call.send(serialize_message(acknowledge_payload_message))] \
           and test_setup.sender_mock.mock_calls == [] \
           and test_setup.clock_mock.mock_calls == [] \
           and test_setup.payload_message_sender_factory_mock.mock_calls == [] \
           and test_setup.payload_message_sender_mocks[0].mock_calls == [call.stop()]


def test_received_acknowledge_payload_twice_after_send_payload():
    test_setup = create_test_setup(number_of_messages=1)
    payload_message, frames = create_payload_message(test_setup, 0)
    test_setup.payload_sender.send_payload(payload_message, frames)
    acknowledge_payload_message = create_acknowledge_payload_message(test_setup, payload_message)
    test_setup.payload_sender.received_acknowledge_payload(message=acknowledge_payload_message.__root__)
    test_setup.reset_mock()
    test_setup.payload_sender.received_acknowledge_payload(message=acknowledge_payload_message.__root__)
    assert test_setup.out_control_socket_mock.mock_calls == [] \
           and test_setup.sender_mock.mock_calls == [] \
           and test_setup.clock_mock.mock_calls == [] \
           and test_setup.payload_message_sender_factory_mock.mock_calls == [] \
           and test_setup.payload_message_sender_mocks[0].mock_calls == []
