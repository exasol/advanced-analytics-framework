import socket

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication.ip_address import IPAddress, Port

NANO_SECOND = 10**-9

LOGGER: FilteringBoundLogger = structlog.getLogger()


class DiscoverySocket:

    def __init__(self, ip_address: IPAddress, port: Port):
        self._port = port
        self._ip_address = ip_address
        self._logger = LOGGER.bind(ip_address=ip_address.model_dump(), port=port.model_dump())
        self._logger.info("create")
        self._udp_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )

    def bind(self):
        self._logger.info("bind")
        self._udp_socket.bind((self._ip_address.ip_address, self._port.port))

    def send(self, message: bytes):
        self._logger.debug("send", message=message)
        self._udp_socket.sendto(message, (self._ip_address.ip_address, self._port.port))

    def recvfrom(self, timeout_in_seconds: float) -> bytes:
        if timeout_in_seconds < 0.0:
            raise ValueError(
                f"Timeout needs to be larger than or equal to 0.0, but got {timeout_in_seconds}"
            )
        # We need to adjust the timeout with a very small number, to avoid 0.0,
        # because this leads the following error
        # BlockingIOError: [Errno 11] Resource temporarily unavailable
        adjusted_timeout = timeout_in_seconds + NANO_SECOND
        self._udp_socket.settimeout(adjusted_timeout)
        data = self._udp_socket.recv(1024)
        self._logger.debug("recvfrom", data=data)
        return data

    def close(self):
        self._logger.info("close")
        try:
            self._udp_socket.close()
        except Exception as e:
            self._logger.exception("Catched exception during self._udp_socket.close")

    def __del__(self):
        self.close()


class DiscoverySocketFactory:
    def create(self, ip_address: IPAddress, port: Port) -> DiscoverySocket:
        return DiscoverySocket(ip_address=ip_address, port=port)
