from typing import Union, Optional
from typing import Union, Optional
from unittest.mock import create_autospec, MagicMock, call

import numpy as np
import pytest
import zmq
from numpy.random import RandomState

from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import PollerFlag, \
    Socket, Frame
from exasol_advanced_analytics_framework.udf_communication.socket_factory.fault_injection_socket_factory import \
    FISocket, FIFrame
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_socket_factory import ZMQSocket
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
    assert mock_cast(socket_mock.send).mock_calls == []


def test_socket_send_no_fault():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send(message)
    assert mock_cast(socket_mock.send).mock_calls == [call(message)]


def test_socket_send_no_fault_bind_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind("inproc://test")
        socket.send(message)
    assert mock_cast(socket_mock.send).mock_calls == [call(message)]


def test_socket_send_no_fault_bind_random_port_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port("inproc://test")
        socket.send(message)
    assert mock_cast(socket_mock.send).mock_calls == [call(message)]


def test_socket_send_no_fault_connect_inproc():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = b"123"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect("inproc://test")
        socket.send(message)
    assert mock_cast(socket_mock.send).mock_calls == [call(message)]


def test_socket_send_mulitpart_fault():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[FIFrame, MagicMock] = create_autospec(FIFrame, spec_set=True)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame_mock]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send).mock_calls == []


def test_socket_send_mulitpart_should_be_fault_but_bind_inproc_is_reliable():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[Frame, MagicMock] = create_autospec(Frame, spec_set=True)
    frame = FIFrame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind("inproc://test")
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send_multipart).mock_calls == [call([frame_mock])]


def test_socket_send_mulitpart_should_be_fault_but_bind_random_port_inproc_is_reliable():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[Frame, MagicMock] = create_autospec(Frame, spec_set=True)
    frame = FIFrame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port("inproc://test")
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send_multipart).mock_calls == [call([frame_mock])]


def test_socket_send_mulitpart_should_be_fault_but_connect_inproc_is_reiliable():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[Frame, MagicMock] = create_autospec(Frame, spec_set=True)
    frame = FIFrame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.09])]
    message = [frame]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect("inproc://test")
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send_multipart).mock_calls == [call([frame_mock])]


def test_socket_send_multipart_no_fault():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    frame_mock: Union[Frame, MagicMock] = create_autospec(Frame, spec_set=True)
    frame = FIFrame(frame_mock)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    mock_cast(random_state_mock.random_sample).side_effect = [np.array([0.1])]
    message = [frame]
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.send_multipart(message)
    assert mock_cast(socket_mock.send_multipart).mock_calls == [call([frame_mock])]


def test_socket_receive():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        message = socket.receive()
    assert message == socket_mock.receive()


def test_socket_bind():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = 'address'
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind(address)
    assert mock_cast(socket_mock.bind).mock_calls == [call(address)]


def test_socket_bind_random_port():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = 'address'
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.bind_to_random_port(address)
    assert mock_cast(socket_mock.bind_to_random_port).mock_calls == [call(address)]


def test_socket_connect():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    address = 'address'
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.connect(address)
    assert mock_cast(socket_mock.connect).mock_calls == [call(address)]


def test_socket_poll():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.poll(PollerFlag.POLLIN, timeout_in_ms=1)
    assert mock_cast(socket_mock.poll).mock_calls == [call(PollerFlag.POLLIN, 1)]


def test_socket_set_identity():
    socket_mock: Union[Socket, MagicMock] = create_autospec(Socket)
    random_state_mock: Union[RandomState, MagicMock] = create_autospec(RandomState)
    name = "test"
    with FISocket(socket_mock, 0.1, random_state_mock) as socket:
        socket.set_identity(name)
    assert mock_cast(socket_mock.set_identity).mock_calls == [call(name)]


@pytest.mark.parametrize("linger", [None, 2])
def test_close_linger(linger: Optional[int]):
    socket_mock: Union[zmq.Socket, MagicMock] = create_autospec(zmq.Socket)
    socket = ZMQSocket(socket_mock)
    socket.close(linger=linger)
    assert mock_cast(socket_mock.close).mock_calls == [call(linger=linger)]


def test_exit_linger():
    socket_mock: Union[zmq.Socket, MagicMock] = create_autospec(zmq.Socket)
    with ZMQSocket(socket_mock) as socket:
        pass
    assert mock_cast(socket_mock.close).mock_calls == [call(linger=None)]


def test_del():
    socket_mock: Union[zmq.Socket, MagicMock] = create_autospec(zmq.Socket)
    socket = ZMQSocket(socket_mock)
    with pytest.warns(ResourceWarning):
        del socket
    assert mock_cast(socket_mock.close).mock_calls == [call(linger=None)]
