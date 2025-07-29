import dataclasses
from typing import Union
from unittest.mock import (
    MagicMock,
    call,
    create_autospec,
)

from test.utils.mock_cast import mock_cast

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.synchronize_connection_sender import (
    SynchronizeConnectionSender,
)
from exasol.analytics.udf.communication.peer_communicator.timer import Timer


@dataclasses.dataclass()
class TestSetup:
    __test__ = False
    peer: Peer
    my_connection_info: ConnectionInfo
    timer_mock: Union[MagicMock, Timer]
    sender_mock: Union[MagicMock, Sender]
    synchronize_connection_sender: SynchronizeConnectionSender

    def reset_mocks(self):
        self.sender_mock.reset_mock()
        self.timer_mock.reset_mock()


def create_test_setup():
    peer = Peer(
        connection_info=ConnectionInfo(
            name="t2",
            ipaddress=IPAddress(ip_address="127.0.0.1"),
            port=Port(port=12),
            group_identifier="g",
        )
    )
    my_connection_info = ConnectionInfo(
        name="t1",
        ipaddress=IPAddress(ip_address="127.0.0.1"),
        port=Port(port=11),
        group_identifier="g",
    )
    timer_mock = create_autospec(Timer)
    sender_mock = create_autospec(Sender)
    synchronize_connection_sender = SynchronizeConnectionSender(
        sender=sender_mock,
        timer=timer_mock,
        my_connection_info=my_connection_info,
        peer=peer,
    )
    return TestSetup(
        peer=peer,
        sender_mock=sender_mock,
        timer_mock=timer_mock,
        my_connection_info=my_connection_info,
        synchronize_connection_sender=synchronize_connection_sender,
    )


def test_init():
    test_setup = create_test_setup()
    assert (
        test_setup.sender_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == []
    )


def test_try_send_after_init_and_is_time_false():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mocks()

    test_setup.synchronize_connection_sender.try_send()

    assert (
        test_setup.sender_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == [call.is_time()]
    )


def test_try_send_after_init_and_is_time_false_and_force():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = False
    test_setup.reset_mocks()

    test_setup.synchronize_connection_sender.try_send(force=True)

    assert test_setup.sender_mock.mock_calls == [
        call.send(
            messages.Message(
                root=messages.SynchronizeConnection(
                    source=test_setup.my_connection_info,
                    destination=test_setup.peer,
                    attempt=1,
                )
            )
        )
    ] and test_setup.timer_mock.mock_calls == [call.is_time(), call.reset_timer()]


def test_try_send_after_init_and_is_time_true():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mocks()

    test_setup.synchronize_connection_sender.try_send()

    assert test_setup.sender_mock.mock_calls == [
        call.send(
            messages.Message(
                root=messages.SynchronizeConnection(
                    source=test_setup.my_connection_info,
                    destination=test_setup.peer,
                    attempt=1,
                )
            )
        )
    ] and test_setup.timer_mock.mock_calls == [call.is_time(), call.reset_timer()]


def test_try_send_twice_and_is_time_true():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.synchronize_connection_sender.try_send()
    test_setup.reset_mocks()

    test_setup.synchronize_connection_sender.try_send()

    assert test_setup.sender_mock.mock_calls == [
        call.send(
            messages.Message(
                root=messages.SynchronizeConnection(
                    source=test_setup.my_connection_info,
                    destination=test_setup.peer,
                    attempt=2,
                )
            )
        )
    ] and test_setup.timer_mock.mock_calls == [call.is_time(), call.reset_timer()]


def test_received_acknowledge_connection_after_init():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.reset_mocks()

    test_setup.synchronize_connection_sender.stop()

    assert (
        test_setup.sender_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == []
    )


def test_received_acknowledge_connection_after_send():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.synchronize_connection_sender.try_send()
    test_setup.reset_mocks()

    test_setup.synchronize_connection_sender.stop()

    assert (
        test_setup.sender_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == []
    )


def test_try_send_after_received_acknowledge_connection_and_is_time_true():
    test_setup = create_test_setup()
    mock_cast(test_setup.timer_mock.is_time).return_value = True
    test_setup.synchronize_connection_sender.stop()
    test_setup.reset_mocks()

    test_setup.synchronize_connection_sender.try_send()

    assert (
        test_setup.sender_mock.mock_calls == []
        and test_setup.timer_mock.mock_calls == [call.is_time()]
    )
