from test.utils.mock_cast import mock_cast
from typing import Union
from unittest.mock import (
    MagicMock,
    call,
    create_autospec,
)

from exasol.analytics.udf.communication.peer_communicator.clock import Clock
from exasol.analytics.udf.communication.peer_communicator.timer import Timer


def test_init():
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)

    timer = Timer(clock=clock_mock, timeout_in_ms=10)

    assert clock_mock.mock_calls == [call.current_timestamp_in_ms()]


def test_is_time_false():
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    mock_cast(clock_mock.current_timestamp_in_ms).side_effect = [0, 10]
    timer = Timer(clock=clock_mock, timeout_in_ms=10)
    clock_mock.reset_mock()

    result = timer.is_time()

    assert result == False and clock_mock.mock_calls == [call.current_timestamp_in_ms()]


def test_is_time_true():
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    mock_cast(clock_mock.current_timestamp_in_ms).side_effect = [0, 11]
    timer = Timer(clock=clock_mock, timeout_in_ms=10)
    clock_mock.reset_mock()

    result = timer.is_time()

    assert result == True and clock_mock.mock_calls == [call.current_timestamp_in_ms()]


def test_is_time_true_after_true():
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    mock_cast(clock_mock.current_timestamp_in_ms).side_effect = [0, 11, 12]
    timer = Timer(clock=clock_mock, timeout_in_ms=10)
    timer.is_time()
    clock_mock.reset_mock()

    result = timer.is_time()

    assert result == True and clock_mock.mock_calls == [call.current_timestamp_in_ms()]


def test_reset_timer():
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    mock_cast(clock_mock.current_timestamp_in_ms).side_effect = [0, 11]
    timer = Timer(clock=clock_mock, timeout_in_ms=10)
    clock_mock.reset_mock()

    timer.reset_timer()

    assert clock_mock.mock_calls == [call.current_timestamp_in_ms()]


def test_it_time_false_after_reset_timer():
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    mock_cast(clock_mock.current_timestamp_in_ms).side_effect = [0, 10, 20]
    timer = Timer(clock=clock_mock, timeout_in_ms=10)
    timer.reset_timer()
    clock_mock.reset_mock()

    result = timer.is_time()

    assert result == False and clock_mock.mock_calls == [call.current_timestamp_in_ms()]


def test_it_time_true_after_reset_timer():
    clock_mock: Union[MagicMock, Clock] = create_autospec(Clock)
    mock_cast(clock_mock.current_timestamp_in_ms).side_effect = [0, 10, 21]
    timer = Timer(clock=clock_mock, timeout_in_ms=10)
    timer.reset_timer()
    clock_mock.reset_mock()

    result = timer.is_time()

    assert result == True and clock_mock.mock_calls == [call.current_timestamp_in_ms()]
