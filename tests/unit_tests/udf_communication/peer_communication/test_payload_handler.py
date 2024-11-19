import dataclasses
from typing import Union
from unittest.mock import MagicMock, call, create_autospec

import pytest
from polyfactory.factories.pydantic_factory import ModelFactory

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.peer_communicator.payload_handler import (
    PayloadHandler,
)
from exasol.analytics.udf.communication.peer_communicator.payload_receiver import (
    PayloadReceiver,
)
from exasol.analytics.udf.communication.peer_communicator.payload_sender import (
    PayloadSender,
)
from exasol.analytics.udf.communication.socket_factory.abstract import Frame
from tests.mock_cast import mock_cast


@dataclasses.dataclass
class TestSetup:
    __test__ = False
    payload_sender_mock: Union[MagicMock, PayloadSender]
    payload_receiver_mock: Union[MagicMock, PayloadReceiver]
    payload_handler: PayloadHandler

    def reset_mock(self):
        self.payload_sender_mock.reset_mock()
        self.payload_receiver_mock.reset_mock()


def create_test_setup() -> TestSetup:
    payload_sender_mock = create_autospec(PayloadSender)
    payload_receiver_mock = create_autospec(PayloadReceiver)
    payload_handler = PayloadHandler(
        payload_sender=payload_sender_mock,
        payload_receiver=payload_receiver_mock,
    )
    return TestSetup(
        payload_handler=payload_handler,
        payload_sender_mock=payload_sender_mock,
        payload_receiver_mock=payload_receiver_mock,
    )


def test_init():
    test_setup = create_test_setup()
    assert (
        test_setup.payload_receiver_mock.mock_calls == []
        and test_setup.payload_sender_mock.mock_calls == []
    )


def test_try_send():
    test_setup = create_resetted_test_setup()
    test_setup.payload_handler.try_send()
    assert (
        test_setup.payload_receiver_mock.mock_calls == []
        and test_setup.payload_sender_mock.mock_calls == [call.try_send()]
    )


def create_resetted_test_setup():
    test_setup = create_test_setup()
    test_setup.reset_mock()
    return test_setup


def test_send_payload():
    test_setup = create_resetted_test_setup()
    frames = [create_autospec(Frame)]
    message = ModelFactory.create_factory(model=messages.Payload).build()
    test_setup.payload_handler.send_payload(message, frames)
    assert (
        test_setup.payload_receiver_mock.mock_calls == []
        and test_setup.payload_sender_mock.mock_calls
        == [call.send_payload(message, frames)]
    )


def test_received_payload():
    test_setup = create_resetted_test_setup()
    frames = [create_autospec(Frame)]
    message = ModelFactory.create_factory(model=messages.Payload).build()
    test_setup.payload_handler.received_payload(message, frames)
    assert (
        test_setup.payload_receiver_mock.mock_calls
        == [call.received_payload(message, frames)]
        and test_setup.payload_sender_mock.mock_calls == []
    )


def test_received_acknowledge_payload():
    test_setup = create_resetted_test_setup()
    message = ModelFactory.create_factory(model=messages.AcknowledgePayload).build()
    test_setup.payload_handler.received_acknowledge_payload(message)
    assert (
        test_setup.payload_receiver_mock.mock_calls == []
        and test_setup.payload_sender_mock.mock_calls
        == [call.received_acknowledge_payload(message)]
    )


@pytest.mark.parametrize(
    "payload_receiver_answer,payload_sender_answer, expected",
    [
        (True, True, True),
        (True, False, False),
        (False, True, False),
        (False, False, False),
    ],
)
def test_is_ready_to_stop(
    payload_receiver_answer: bool, payload_sender_answer: bool, expected: bool
):
    test_setup = create_resetted_test_setup()
    mock_cast(test_setup.payload_sender_mock.is_ready_to_stop).return_value = (
        payload_sender_answer
    )
    mock_cast(test_setup.payload_receiver_mock.is_ready_to_stop).return_value = (
        payload_receiver_answer
    )
    result = test_setup.payload_handler.is_ready_to_stop()
    assert (
        result == expected
        and test_setup.payload_receiver_mock.mock_calls == [call.is_ready_to_stop()]
        and test_setup.payload_sender_mock.mock_calls == [call.is_ready_to_stop()]
    )
