import socket

from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port


class LocalDiscoverySocket:

    def __init__(self, port: Port, timeout: int):
        self._port = port
        self._ip = IPAddress(ip_address="127.0.0.1")
        self._broadcast_ip = IPAddress(ip_address="127.255.255.255")
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self._udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._udp_socket.settimeout(timeout)
        self._udp_socket.bind((self._ip, self._port))

    def send(self, message: bytes):
        self._udp_socket.sendto(message, (self._broadcast_ip, self._port))

    def recvfrom(self) -> bytes:
        data = self._udp_socket.recv(1024)
        return data

    def close(self):
        try:
            self._udp_socket.close()
        except:
            pass

    def __del__(self):
        self.close()
