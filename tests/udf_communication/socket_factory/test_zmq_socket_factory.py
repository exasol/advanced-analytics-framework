import pytest
import zmq

from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import SocketType
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_socket_factory import ZMQSocketFactory, \
    ZMQSocket, ZMQFrame, ZMQPoller


@pytest.mark.parametrize("socket_type,zmq_socket_type",
                         [
                             (SocketType.PAIR, zmq.PAIR),
                             (SocketType.DEALER, zmq.DEALER),
                             (SocketType.ROUTER, zmq.ROUTER)
                         ])
def test_create_socket(socket_type: SocketType, zmq_socket_type):
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        socket = factory.create_socket(socket_type)
        assert isinstance(socket, ZMQSocket) \
               and isinstance(socket._internal_socket, zmq.Socket) \
               and socket._internal_socket.type == zmq_socket_type

def test_create_frame():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        value = b"123"
        frame = factory.create_frame(value)
        assert isinstance(frame, ZMQFrame) \
               and frame.to_bytes() == value \
               and isinstance(frame._internal_frame, zmq.Frame) \
               and frame._internal_frame.bytes == value


def test_create_poller():
    with  zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        poller = factory.create_poller()
        assert isinstance(poller, ZMQPoller) and isinstance(poller._internal_poller, zmq.Poller)
