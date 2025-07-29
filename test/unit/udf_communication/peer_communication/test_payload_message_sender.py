import dataclasses
from typing import (
    List,
    Union,
)
from unittest.mock import (
    MagicMock,
    call,
    create_autospec,
)

import pytest
from test.utils.mock_cast import mock_cast

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.payload_message_sender import (
    PayloadMessageSender,
)
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.timer import Timer
from exasol.analytics.udf.communication.serialization import serialize_message
from exasol.analytics.udf.communication.socket_factory.abstract import (
    Frame,
    Socket,
)


@dataclasses.dataclass(frozen=True)
class TestSetup:
    __test__ = False
    sender_mock: Union[Sender, MagicMock]
    abort_time_mock: Union[Timer, MagicMock]
    out_control_socket_mock: Union[Socket, MagicMock]
    retry_timer_mock: Union[Timer, MagicMock]
    frame_mocks: list[Union[Frame, MagicMock]]
    message: messages.Payload
    payload_message_sender: PayloadMessageSender

    def reset_mocks(self):
        for frame_mock in self.frame_mocks:
            frame_mock.reset_mock()
        self.sender_mock.reset_mock()
        self.out_control_socket_mock.reset_mock()
        self.abort_time_mock.reset_mock()
        self.retry_timer_mock.reset_mock()


def create_test_setup() -> TestSetup:
    sender_mock = create_autospec(Sender)
    abort_time_mock = create_autospec(Timer)
    out_control_socket_mock = create_autospec(Socket)
    retry_timer_mock = create_autospec(Timer)
    frame_mocks = [create_autospec(Frame)]
    message = messages.Payload(
        source=Peer(
            connection_info=ConnectionInfo(
                name="t1",
                ipaddress=IPAddress(ip_address="127.0.0.1"),
                port=Port(port=1000),
                group_identifier="group",
            )
        ),
        destination=Peer(
            connection_info=ConnectionInfo(
                name="t1",
                ipaddress=IPAddress(ip_address="127.0.0.1"),
                port=Port(port=1000),
                group_identifier="group",
            )
        ),
        sequence_number=0,
    )
    payload_message_sender = PayloadMessageSender(
        sender=sender_mock,
        abort_timer=abort_time_mock,
        retry_timer=retry_timer_mock,
        out_control_socket=out_control_socket_mock,
        message=message,
        frames=frame_mocks,
    )
    return TestSetup(
        message=message,
        sender_mock=sender_mock,
        frame_mocks=frame_mocks,
        out_control_socket_mock=out_control_socket_mock,
        abort_time_mock=abort_time_mock,
        retry_timer_mock=retry_timer_mock,
        payload_message_sender=payload_message_sender,
    )


def test_init():
    test_setup = create_test_setup()
    assert (
        test_setup.out_control_socket_mock.mock_calls == []
        and mock_cast(test_setup.sender_mock.send_multipart).mock_calls
        == [call(test_setup.frame_mocks)]
        and test_setup.retry_timer_mock.mock_calls == []
        and test_setup.abort_time_mock.mock_calls == []
    )


@pytest.mark.parametrize("is_retry_time", [True, False])
def test_try_send_abort_timer_is_time_true(is_retry_time: bool):
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    mock_cast(test_setup.abort_time_mock.is_time).return_value = True
    mock_cast(test_setup.retry_timer_mock.is_time).return_value = is_retry_time
    test_setup.payload_message_sender.try_send()
    abort_payload = messages.AbortPayload(
        payload=test_setup.message, reason="Send timeout reached"
    )
    assert (
        mock_cast(test_setup.out_control_socket_mock.send).mock_calls
        == [call(serialize_message(abort_payload))]
        and test_setup.sender_mock.mock_calls == []
        and test_setup.retry_timer_mock.mock_calls == []
        and test_setup.abort_time_mock.mock_calls == [call.is_time()]
    )


def test_try_send_abort_timer_is_time_false_retry_timer_is_time_false():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    mock_cast(test_setup.abort_time_mock.is_time).return_value = False
    mock_cast(test_setup.retry_timer_mock.is_time).return_value = False
    test_setup.payload_message_sender.try_send()
    assert (
        mock_cast(test_setup.out_control_socket_mock.send).mock_calls == []
        and test_setup.sender_mock.mock_calls == []
        and test_setup.retry_timer_mock.mock_calls == [call.is_time()]
        and test_setup.abort_time_mock.mock_calls == [call.is_time()]
    )


def test_try_send_abort_timer_is_time_false_retry_timer_is_time_true():
    test_setup = create_test_setup()
    test_setup.reset_mocks()
    mock_cast(test_setup.abort_time_mock.is_time).return_value = False
    mock_cast(test_setup.retry_timer_mock.is_time).return_value = True
    test_setup.payload_message_sender.try_send()
    assert (
        mock_cast(test_setup.out_control_socket_mock.send).mock_calls == []
        and mock_cast(test_setup.sender_mock.send_multipart).mock_calls
        == [call(test_setup.frame_mocks)]
        and test_setup.retry_timer_mock.mock_calls
        == [call.is_time(), call.reset_timer()]
        and test_setup.abort_time_mock.mock_calls == [call.is_time()]
    )


@pytest.mark.parametrize(
    ["is_retry_time", "is_abort_time"],
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_try_send_after_stop(is_retry_time: bool, is_abort_time: bool):
    test_setup = create_test_setup()
    test_setup.payload_message_sender.stop()
    test_setup.reset_mocks()
    mock_cast(test_setup.abort_time_mock.is_time).return_value = is_abort_time
    mock_cast(test_setup.retry_timer_mock.is_time).return_value = is_retry_time
    test_setup.payload_message_sender.try_send()
    assert (
        test_setup.out_control_socket_mock.mock_calls == []
        and test_setup.sender_mock.mock_calls == []
        and test_setup.retry_timer_mock.mock_calls == [call.is_time()]
        and test_setup.abort_time_mock.mock_calls == [call.is_time()]
    )
