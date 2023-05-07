import socket

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port

NANO_SECOND = 10 ** -9

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class GlobalDiscoverySocket:

    def __init__(self, ip_address: IPAddress, port: Port):
        self._port = port
        self._ip_address = ip_address
        self._logger = LOGGER.bind(
            port=self._port.dict(),
            ip_address=self._ip_address.dict()
        )
        self._logger.info("init")
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    def bind(self):
        self._logger.info("bind")
        self._udp_socket.bind((self._ip_address.ip_address, self._port.port))

    def send(self, message: bytes):
        self._logger.debug("send", message=message)
        self._udp_socket.sendto(message, (self._ip_address.ip_address, self._port.port))

    def recvfrom(self, timeout_in_seconds: float) -> bytes:
        logger = self._logger.bind(timeout_in_seconds=timeout_in_seconds)
        logger.debug("recvfrom")
        if timeout_in_seconds < 0.0:
            raise ValueError(f"Timeout needs to be larger than or equal to 0.0, but got {timeout_in_seconds}")
        # We need to adjust the timeout with a very small number, to avoid 0.0,
        # because this leads the following error
        # BlockingIOError: [Errno 11] Resource temporarily unavailable
        adjusted_timeout = timeout_in_seconds + NANO_SECOND
        self._udp_socket.settimeout(adjusted_timeout)
        data = self._udp_socket.recv(1024)
        logger.debug("recieved", message=data)
        return data

    def close(self):
        self._logger.info("close")
        try:
            self._udp_socket.close()
        except:
            pass

    def __del__(self):
        self.close()


class GlobalDiscoverySocketFactory:
    def create(self, ip_address: IPAddress, port: Port) -> GlobalDiscoverySocket:
        return GlobalDiscoverySocket(ip_address=ip_address, port=port)
