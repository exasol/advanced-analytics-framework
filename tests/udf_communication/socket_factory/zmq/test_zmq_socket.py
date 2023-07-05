from typing import Union, Optional
from typing import Union, Optional
from unittest.mock import create_autospec, MagicMock

import pytest
import zmq
from zmq import ZMQError

from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketType, \
    PollerFlag
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_wrapper import ZMQSocketFactory, \
    ZMQFrame, ZMQSocket
from tests.mock_cast import mock_cast


def test_create_socket_with():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.PAIR) as socket1:
            with factory.create_socket(SocketType.PAIR) as socket2:
                socket1.bind("inproc://test")
                socket2.connect("inproc://test")
                socket1.send(b"123")
            with pytest.raises(ZMQError):
                socket2.receive()


def test_socket_send_receive():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.PAIR) as socket1:
            with factory.create_socket(SocketType.PAIR) as socket2:
                socket1.bind("inproc://test")
                socket2.connect("inproc://test")
                value = b"123"
                socket1.send(value)
                result = socket2.receive()
                assert value == result


def test_socket_bind_random_port():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.ROUTER) as socket1:
            with factory.create_socket(SocketType.DEALER) as socket2:
                port = socket1.bind_to_random_port("tcp://127.0.0.1")
                socket2.connect(f"tcp://127.0.0.1:{port}")
                value = b"123"
                socket2.send(value)
                result = socket1.receive_multipart()
                assert value == result[1].to_bytes()


def test_socket_send_receive_multipart():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.PAIR) as socket1:
            with factory.create_socket(SocketType.PAIR) as socket2:
                socket1.bind("inproc://test")
                socket2.connect("inproc://test")
                input_message = [
                    factory.create_frame(b"123"),
                    factory.create_frame(b"456")
                ]
                socket1.send_multipart(input_message)
                output_message = socket2.receive_multipart()
                input_message_bytes = [frame.to_bytes() for frame in input_message]
                input_message_type = [type(frame) for frame in input_message]
                output_message_bytes = [frame.to_bytes() for frame in output_message]
                output_message_type = [type(frame) for frame in output_message]
                assert input_message_bytes == output_message_bytes \
                       and input_message_type == output_message_type \
                       and input_message_type[0] == ZMQFrame


def test_socket_poll_in():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.PAIR) as socket1:
            with factory.create_socket(SocketType.PAIR) as socket2:
                socket1.bind("inproc://test")
                socket2.connect("inproc://test")
                socket1.send(b"123")
                result = socket2.poll(PollerFlag.POLLIN)
                assert result == {PollerFlag.POLLIN}


def test_socket_poll_out():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.PAIR) as socket1:
            with factory.create_socket(SocketType.PAIR) as socket2:
                socket1.bind("inproc://test")
                socket2.connect("inproc://test")
                result = socket1.poll(PollerFlag.POLLOUT)
                assert result == {PollerFlag.POLLOUT}


def test_socket_poll_in_out():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.PAIR) as socket1:
            with factory.create_socket(SocketType.PAIR) as socket2:
                socket1.bind("inproc://test")
                socket2.connect("inproc://test")
                socket1.send(b"123")
                result = socket2.poll({PollerFlag.POLLOUT, PollerFlag.POLLIN})
                assert result == {PollerFlag.POLLOUT, PollerFlag.POLLIN}


def test_socket_set_identity():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.PAIR) as socket1:
            name = "test"
            socket1.set_identity(name)
            if isinstance(socket1, ZMQSocket):
                result = socket1._internal_socket.get_string(zmq.IDENTITY)
            assert result == name


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
