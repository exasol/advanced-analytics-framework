import zmq

from exasol.analytics.udf.communication.socket_factory.abstract import (
    PollerFlag,
    SocketType,
)
from exasol.analytics.udf.communication.socket_factory.zmq_wrapper import (
    ZMQSocketFactory,
)


def test_create_poller():
    with zmq.Context() as context:
        factory = ZMQSocketFactory(context)
        with factory.create_socket(SocketType.PAIR) as socket1, factory.create_socket(
            SocketType.PAIR
        ) as socket2, factory.create_socket(
            SocketType.PAIR
        ) as socket3, factory.create_socket(
            SocketType.PAIR
        ) as socket4:
            socket1.bind("inproc://test1")
            socket2.connect("inproc://test1")
            socket3.bind("inproc://test2")
            socket4.connect("inproc://test2")
            socket1.send(b"123")
            socket3.send(b"456")
            poller = factory.create_poller()
            poller.register(socket1, flags=PollerFlag.POLLIN)
            poller.register(socket2, flags=PollerFlag.POLLIN)
            poller.register(socket3, flags=PollerFlag.POLLIN)
            poller.register(socket4, flags=PollerFlag.POLLIN)
            result = poller.poll()
            assert result == {
                socket2: {PollerFlag.POLLIN},
                socket4: {PollerFlag.POLLIN},
            }
