import socket
from typing import Tuple

from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port


class GlobalDiscoverySocket:

    def __init__(self, bind_address: IPAddress, port: Port):
        self._port = port
        self._bind_address = bind_address

    def send(self, ip: IPAddress, message: bytes):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            udp_socket.bind((self._bind_address, self._port))
            udp_socket.sendto(message, (ip, self._port))
        finally:
            udp_socket.close()

    def recvfrom(self) -> Tuple[bytes, IPAddress]:
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            udp_socket.bind((self._bind_address, self._port))
            data, addr = udp_socket.recvfrom(1024)
            return data, IPAddress(addr)
        finally:
            udp_socket.close()