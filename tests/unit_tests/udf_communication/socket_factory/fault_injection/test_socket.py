from typing import Optional, Union
from unittest.mock import MagicMock, call, create_autospec

import numpy as np
import pytest
from numpy.random import RandomState

from exasol.analytics.udf.communication.socket_factory import abstract, fault_injection
from tests.mock_cast import mock_cast


def test_create_socket_with():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        pass
    mock_cast(socket_mock.close).assert_called_once_with(linger=None)


def test_socket_send_fault():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send(b"123")
    assert mock_cast(socket_mock.send).mock_calls == []


def test_socket_send_no_fault():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send(message)
    assert mock_cast(socket_mock.send).mock_calls == [call(message)]


def test_socket_send_no_fault_bind_inproc():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind("inproc://test")
        socket.send(message)
    assert mock_cast(socket_mock.send).mock_calls == [call(message)]


def test_socket_send_no_fault_bind_random_port_inproc():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port("inproc://test")
        socket.send(message)
    assert mock_cast(socket_mock.send).mock_calls == [call(message)]


def test_socket_send_no_fault_connect_inproc():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect("inproc://test")
        socket.send(message)
    assert mock_cast(socket_mock.send).mock_calls == [call(message)]


def test_socket_send_mulitpart_fault():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    frame_mock: Union[abstract.Frame, MagicMock] = create_autospec(
        abstract.Frame, spec_set=True
    )
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame_mock]
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send).mock_calls == []


def test_socket_send_mulitpart_should_be_fault_but_bind_inproc_is_reliable():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    frame_mock: Union[abstract.Frame, MagicMock] = create_autospec(
        abstract.Frame, spec_set=True
    )
    frame = fault_injection.Frame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind("inproc://test")
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send_multipart).mock_calls == [call([frame_mock])]


def test_socket_send_mulitpart_should_be_fault_but_bind_random_port_inproc_is_reliable():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    frame_mock: Union[abstract.Frame, MagicMock] = create_autospec(
        abstract.Frame, spec_set=True
    )
    frame = fault_injection.Frame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port("inproc://test")
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send_multipart).mock_calls == [call([frame_mock])]


def test_socket_send_mulitpart_should_be_fault_but_connect_inproc_is_reiliable():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    frame_mock: Union[abstract.Frame, MagicMock] = create_autospec(
        abstract.Frame, spec_set=True
    )
    frame = fault_injection.Frame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect("inproc://test")
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send_multipart).mock_calls == [call([frame_mock])]


def test_socket_send_multipart_no_fault():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    frame_mock: Union[abstract.Frame, MagicMock] = create_autospec(
        abstract.Frame, spec_set=True
    )
    frame = fault_injection.Frame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = [frame]
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send_multipart).mock_calls == [call([frame_mock])]


def test_socket_receive():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        message = socket.receive()
    assert message == socket_mock.receive()


def test_socket_bind():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = "address"
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind(address)
    assert mock_cast(socket_mock.bind).mock_calls == [call(address)]


def test_socket_bind_random_port():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = "address"
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port(address)
    assert mock_cast(socket_mock.bind_to_random_port).mock_calls == [call(address)]


def test_socket_connect():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = "address"
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect(address)
    assert mock_cast(socket_mock.connect).mock_calls == [call(address)]


def test_socket_poll():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.poll(abstract.PollerFlag.POLLIN, timeout_in_ms=1)
    assert mock_cast(socket_mock.poll).mock_calls == [
        call(abstract.PollerFlag.POLLIN, 1)
    ]


def test_socket_set_identity():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    name = "test"
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        socket.set_identity(name)
    assert mock_cast(socket_mock.set_identity).mock_calls == [call(name)]


@pytest.mark.parametrize("linger", [None, 2])
def test_close_linger(linger: Optional[int]):
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    socket = fault_injection.Socket(socket_mock, 0.1, random_state_mock)
    socket.close(linger=linger)
    assert mock_cast(socket_mock.close).mock_calls == [call(linger=linger)]


def test_exit_linger():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with fault_injection.Socket(socket_mock, 0.1, random_state_mock) as socket:
        pass
    assert mock_cast(socket_mock.close).mock_calls == [call(linger=None)]


def test_del():
    socket_mock: Union[abstract.Socket, MagicMock] = create_autospec(abstract.Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    socket = fault_injection.Socket(socket_mock, 0.1, random_state_mock)
    with pytest.warns(ResourceWarning):
        del socket
    assert mock_cast(socket_mock.close).mock_calls == [call(linger=None)]
