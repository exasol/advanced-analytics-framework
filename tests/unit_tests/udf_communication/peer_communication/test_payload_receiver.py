import dataclasses
from typing import Union, Tuple, List
from unittest.mock import create_autospec, MagicMock, call

import pytest

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_receiver import PayloadReceiver
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket, Frame


@dataclasses.dataclass
class TestSetup:
    sender_mock: Union[MagicMock, Sender]
    out_control_socket_mock: Union[MagicMock, Socket]
    my_connection_info: ConnectionInfo
    peer: Peer
    payload_receiver: PayloadReceiver

    def reset_mock(self):
        self.out_control_socket_mock.reset_mock()
        self.sender_mock.reset_mock()


def create_test_setup() -> TestSetup:
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
    payload_receiver = PayloadReceiver(sender=sender_mock,
                                       out_control_socket=out_control_socket_mock,
                                       my_connection_info=my_connection_info,
                                       peer=peer)
    return TestSetup(
        peer=peer,
        my_connection_info=my_connection_info,
        sender_mock=sender_mock,
        out_control_socket_mock=out_control_socket_mock,
        payload_receiver=payload_receiver
    )


def create_acknowledge_payload_message(test_setup: TestSetup, message: messages.Payload) -> messages.Message:
    acknowledge_message = messages.Message(__root__=messages.AcknowledgePayload(
        source=Peer(connection_info=test_setup.my_connection_info),
        sequence_number=message.sequence_number))
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
    test_setup = create_test_setup()
    assert test_setup.out_control_socket_mock.mock_calls == [] \
           and test_setup.sender_mock.mock_calls == []


@pytest.mark.parametrize("number_of_messages", [i for i in range(1, 10)])
def test_received_payload_in_sequence(number_of_messages: int):
    test_setup = create_test_setup()
    for sequence_number in range(number_of_messages - 1):
        print(sequence_number)
        message, frames = create_payload_message(test_setup, sequence_number)
        test_setup.payload_receiver.received_payload(message, frames)
    test_setup.reset_mock()
    sequence_number = number_of_messages - 1
    message, frames = create_payload_message(test_setup, sequence_number)
    test_setup.payload_receiver.received_payload(message, frames)
    acknowledge_message = create_acknowledge_payload_message(test_setup, message)
    assert test_setup.out_control_socket_mock.mock_calls == [call.send_multipart(frames)] \
           and test_setup.sender_mock.mock_calls == [call.send(message=acknowledge_message)]


@pytest.mark.parametrize("number_of_messages", [i for i in range(1, 10)])
def test_received_payload_in_reverse_sequence(number_of_messages: int):
    test_setup = create_test_setup()
    frames_of_previous_message = []
    for sequence_number in range(number_of_messages - 1, 0, -1):
        message, frames = create_payload_message(test_setup, sequence_number)
        frames_of_previous_message.append(frames)
        test_setup.payload_receiver.received_payload(message, frames)
    test_setup.reset_mock()
    sequence_number = 0
    message, frames = create_payload_message(test_setup, sequence_number)
    test_setup.payload_receiver.received_payload(message, frames)
    acknowledge_message = create_acknowledge_payload_message(test_setup, message)
    send_of_previous_messages = [call.send_multipart(frames) for frames in reversed(frames_of_previous_message)]
    out_control_mock_calls = [call.send_multipart(frames)] + send_of_previous_messages
    assert test_setup.out_control_socket_mock.mock_calls == out_control_mock_calls \
           and test_setup.sender_mock.mock_calls == [call.send(message=acknowledge_message)]


@pytest.mark.parametrize("number_of_messages, duplicated_message",
                         [(i, j) for i in range(1, 10) for j in range(0, i)])
def test_received_payload_in_sequence_multiple_times(number_of_messages: int, duplicated_message: int):
    test_setup = create_test_setup()
    for sequence_number in range(number_of_messages):
        message, frames = create_payload_message(test_setup, sequence_number)
        test_setup.payload_receiver.received_payload(message, frames)
    test_setup.reset_mock()
    sequence_number = duplicated_message
    message, frames = create_payload_message(test_setup, sequence_number)
    test_setup.payload_receiver.received_payload(message, frames)
    acknowledge_message = create_acknowledge_payload_message(test_setup, message)
    assert test_setup.out_control_socket_mock.mock_calls == [] \
           and test_setup.sender_mock.mock_calls == [call.send(message=acknowledge_message)]


@pytest.mark.parametrize("number_of_messages, duplicated_message",
                         [(i, j) for i in range(1, 10) for j in range(1, i)])
def test_received_payload_in_reverse_sequence_incomplete_multiple_times(
        number_of_messages: int, duplicated_message: int):
    test_setup = create_test_setup()
    frames_of_previous_message = []
    for sequence_number in range(number_of_messages - 1, 0, -1):
        message, frames = create_payload_message(test_setup, sequence_number)
        frames_of_previous_message.append(frames)
        test_setup.payload_receiver.received_payload(message, frames)
    test_setup.reset_mock()
    sequence_number = duplicated_message
    message, frames = create_payload_message(test_setup, sequence_number)
    test_setup.payload_receiver.received_payload(message, frames)
    acknowledge_message = create_acknowledge_payload_message(test_setup, message)
    assert test_setup.out_control_socket_mock.mock_calls == [] \
           and test_setup.sender_mock.mock_calls == [call.send(message=acknowledge_message)]


@pytest.mark.parametrize("number_of_messages", [i for i in range(1, 10)])
def test_is_ready_to_close_after_received_payload_in_sequence(number_of_messages: int):
    test_setup = create_test_setup()
    for sequence_number in range(number_of_messages):
        print(sequence_number)
        message, frames = create_payload_message(test_setup, sequence_number)
        test_setup.payload_receiver.received_payload(message, frames)
    test_setup.reset_mock()
    result = test_setup.payload_receiver.is_ready_to_stop()
    assert result


@pytest.mark.parametrize("number_of_messages", [i for i in range(1, 10)])
def test_is_ready_to_stop_after_received_payload_in_reverse_sequence(number_of_messages: int):
    test_setup = create_test_setup()
    frames_of_previous_message = []
    for sequence_number in range(number_of_messages - 1, -1, -1):
        message, frames = create_payload_message(test_setup, sequence_number)
        frames_of_previous_message.append(frames)
        test_setup.payload_receiver.received_payload(message, frames)
    test_setup.reset_mock()
    is_ready_to_stop = test_setup.payload_receiver.is_ready_to_stop()
    assert is_ready_to_stop


@pytest.mark.parametrize("number_of_messages", [i for i in range(2, 10)])
def test_is_ready_to_stop_after_received_payload_in_reverse_sequence_incomplete(number_of_messages: int):
    test_setup = create_test_setup()
    frames_of_previous_message = []
    for sequence_number in range(number_of_messages - 1, 0, -1):
        message, frames = create_payload_message(test_setup, sequence_number)
        frames_of_previous_message.append(frames)
        test_setup.payload_receiver.received_payload(message, frames)
    test_setup.reset_mock()
    is_ready_to_stop = test_setup.payload_receiver.is_ready_to_stop()
    assert not is_ready_to_stop
