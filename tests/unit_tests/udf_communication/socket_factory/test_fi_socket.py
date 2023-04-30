import time
from typing import Union, Optional
from unittest.mock import create_autospec, MagicMock, Mock

import numpy as np
import pytest
import zmq
from numpy.random import RandomState
from zmq import ZMQError

from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import SocketType, \
    PollerFlag, Socket, Frame
from exasol_advanced_analytics_framework.udf_communication.socket_factory.fault_injection_socket_factory import \
    FISocket, FIFrame
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_socket_factory import ZMQSocketFactory, \
    ZMQFrame, ZMQSocket
from tests.mock_cast import mock_cast


def test_create_socket_with():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        pass
    mock_cast(socket_mock.close).assert_called_once_with(linger=None)


def test_socket_send_fault():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send(b"123")
    mock_cast(socket_mock.send).assert_not_called()


def test_socket_send_no_fault():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send(message)
    mock_cast(socket_mock.send).assert_called_once_with(message)


def test_socket_send_no_fault_bind_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind("inproc://test")
        socket.send(message)
    mock_cast(socket_mock.send).assert_called_once_with(message)


def test_socket_send_no_fault_bind_random_port_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port("inproc://test")
        socket.send(message)
    mock_cast(socket_mock.send).assert_called_once_with(message)


def test_socket_send_no_fault_connect_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect("inproc://test")
        socket.send(message)
    mock_cast(socket_mock.send).assert_called_once_with(message)


def test_socket_send_mulitpart_fault():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[FIFrame, MagicMock] = create_autospec(FIFrame, spec_set=True)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame_mock]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send_multipart(message)
    mock_cast(socket_mock.send).assert_not_called()


def test_socket_send_mulitpart_fault_bind_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[Frame, MagicMock] = create_autospec(Frame, spec_set=True)
    frame = FIFrame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind("inproc://test")
        socket.send_multipart(message)
    mock_cast(socket_mock.send_multipart).assert_called_once_with([frame_mock])


def test_socket_send_mulitpart_fault_bind_random_port_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[Frame, MagicMock] = create_autospec(Frame, spec_set=True)
    frame = FIFrame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port("inproc://test")
        socket.send_multipart(message)
    mock_cast(socket_mock.send_multipart).assert_called_once_with([frame_mock])


def test_socket_send_mulitpart_fault_connect_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[Frame, MagicMock] = create_autospec(Frame, spec_set=True)
    frame = FIFrame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect("inproc://test")
        socket.send_multipart(message)
    mock_cast(socket_mock.send_multipart).assert_called_once_with([frame_mock])


def test_socket_send_multipart_no_fault():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[Frame, MagicMock] = create_autospec(Frame, spec_set=True)
    frame = FIFrame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = [frame]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send_multipart(message)
    mock_cast(socket_mock.send_multipart).assert_called_once_with([frame_mock])


def test_socket_receive():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        message = socket.receive()
    mock_cast(socket_mock.receive).assert_called_once()
    assert message == socket_mock.receive()


def test_socket_bind():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = 'address'
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind(address)
    mock_cast(socket_mock.bind).assert_called_once_with(address)


def test_socket_bind_random_port():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = 'address'
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port(address)
    mock_cast(socket_mock.bind_to_random_port).assert_called_once_with(address)


def test_socket_connect():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = 'address'
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect(address)
    mock_cast(socket_mock.connect).assert_called_once_with(address)


def test_socket_poll():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.poll(PollerFlag.POLLIN, timeout_in_ms=1)
    mock_cast(socket_mock.poll).assert_called_once_with(PollerFlag.POLLIN, 1)


def test_socket_set_identity():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    name = "test"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.set_identity(name)
    mock_cast(socket_mock.set_identity).assert_called_once_with(name)


@pytest.mark.parametrize("linger", [None, 2])
def test_close_linger(linger: Optional[int]):
    socket_mock: Union[zmq.Socket, MagicMock] = create_autospec(zmq.Socket)
    socket = ZMQSocket(socket_mock)
    socket.close(linger=linger)
    mock_cast(socket_mock.close).assert_called_once_with(linger=linger)


def test_exit_linger():
    socket_mock: Union[zmq.Socket, MagicMock] = create_autospec(zmq.Socket)
    with ZMQSocket(socket_mock) as socket:
        pass
    mock_cast(socket_mock.close).assert_called_once_with(linger=None)


def test_del():
    socket_mock: Union[zmq.Socket, MagicMock] = create_autospec(zmq.Socket)
    socket = ZMQSocket(socket_mock)
    with pytest.warns(ResourceWarning):
        del socket
    mock_cast(socket_mock.close).assert_called_once_with(linger=None)
