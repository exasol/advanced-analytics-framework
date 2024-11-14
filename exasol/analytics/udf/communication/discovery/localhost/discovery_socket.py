import socket

from exasol.analytics.udf.communication.ip_address import IPAddress, Port


class DiscoverySocket:

    def __init__(self, port: Port):
        self._port = port
        self._broadcast_ip = IPAddress(ip_address="127.255.255.255")
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self._udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._udp_socket.bind((self._broadcast_ip.ip_address, self._port.port))

    def send(self, message: bytes):
        self._udp_socket.sendto(message, (self._broadcast_ip.ip_address, self._port.port))

    def recvfrom(self, timeout_in_seconds: float) -> bytes:
        if timeout_in_seconds < 0.0:
            raise ValueError(f"Timeout needs to be larger than or equal to 0.0, but got {timeout_in_seconds}")
        # We need to adjust the timeout with a very small number, to avoid 0.0,
        # because this leads the following error
        # BlockingIOError: [Errno 11] Resource temporarily unavailable
        adjusted_timeout = timeout_in_seconds + 10 ** -9
        self._udp_socket.settimeout(adjusted_timeout)
        data = self._udp_socket.recv(1024)
        return data

    def close(self):
        try:
            self._udp_socket.close()
        except:
            pass

    def __del__(self):
        self.close()


class DiscoverySocketFactory:
    def create(self, port: Port) -> DiscoverySocket:
        return DiscoverySocket(port=port)
