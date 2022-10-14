import threading
from typing import Set, Optional

import zmq

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port


class Peer:

    def __init__(self, context: zmq.Context, connection_info: ConnectionInfo):
        self._connection_info = connection_info
        self._socket = context.socket(zmq.DEALER)
        self._socket.connect(f"{connection_info.ipaddress}:{connection_info.port}")
        self._pipe_socket = context.socket(zmq.PAIR)
        self.pipe_address = f"inproc://Peer{id(self)}"
        self._pipe_socket.bind(self.pipe_address)

    def send(self, message: bytes):
        pass

    def recv(self, timeout=None) -> Optional[bytes]:
        result = self._socket.poll(timeout=timeout)
        if result == 0:
            return None
        else:
            return self._socket.recv()


class BackgroundListener:

    def __init__(self, context: zmq.Context, listen_ip: IPAddress, group_identifier: str):
        self._group_identifier = group_identifier
        self._listen_ip = listen_ip
        self._context = context
        self._control_socket = context.socket(zmq.PAIR)
        self._control_socket_address = f"inproc://BackgroundListener{id(self)}"
        self._control_socket.bind(self._control_socket)
        self._my_connection_info: Optional[ConnectionInfo] = None
        self._my_connection_info_ready_event = threading.Event()
        self._thread = threading.Thread(target=self.run)
        self._thread.start()

    @property
    def my_connection_info(self) -> ConnectionInfo:
        self._my_connection_info_ready_event.wait()
        return self._my_connection_info

    def stop(self):
        self._control_socket.send("STOP")

    def run(self):
        control_socket = self._context.socket(zmq.PAIR)
        control_socket.connect(self._control_socket)
        listener_socket = self._context.socket(zmq.ROUTER)
        port = listener_socket.bind_to_random_port(f"tcp://*")
        self._set_my_connection_info(port)
        stopped = False
        poller = zmq.Poller()
        poller.register(control_socket, zmq.POLLIN)
        poller.register(listener_socket, zmq.POLLIN)
        while not stopped:
            socks = dict(poller.poll())
            if control_socket in socks and socks[control_socket] == zmq.POLLIN:
                message = control_socket.recv()
                stopped = self._handle_control_message(message)
            if listener_socket in socks and socks[listener_socket] == zmq.POLLIN:
                message = listener_socket.recv()
                self._handle_listener_message(message)

    def _handle_control_message(self, message: bytes):
        if message == "STOP":
            pass
        elif message == "ADD_PEER":
            pass
        else:
            # ignore and log
            pass

    def _handle_listener_message(self, message: bytes):
        if message == "Pong":
            pass
        elif message == "Payload":
            pass
        else:
            # ignore and log
            pass

    def _set_my_connection_info(self, port):
        self._my_connection_info = ConnectionInfo(
            ipaddress=self._listen_ip,
            port=Port(port=port),
            group_identifier=self._group_identifier
        )
        self._my_connection_info_ready_event.set()

    def add_peer(self, peer: Peer):
        self._control_socket.send(f"ADD_PEER:{peer.pipe_address}")


class PeerCommunicator:

    def __init__(self, number_of_peers: int, listen_ip: IPAddress, group_identifier: str):
        self._number_of_peers = number_of_peers
        self._context = zmq.Context()
        self._background_listener = BackgroundListener(context=self._context,
                                                       listen_ip=listen_ip,
                                                       group_identifier=group_identifier)
        self._my_connection_info = self._background_listener.my_connection_info
        self._local_peers: Set[Peer] = set()

    def add_peer(self, peer_connection_info: ConnectionInfo):
        if (peer_connection_info.ipaddress == self._my_connection_info
                and peer_connection_info.group_identifier == self._my_connection_info.group_identifier
                and peer_connection_info.port != self._my_connection_info.port):
            peer = Peer(self._context, peer_connection_info)
            self._background_listener.add_peer(peer)
            self._local_peers.add(peer)

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def are_all_peers_connected(self) -> bool:
        return len(self._local_peers) < self._number_of_peers - 1
