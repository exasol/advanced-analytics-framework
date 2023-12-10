from typing import Protocol

from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory


class SocketFactoryContextManager(Protocol):

    def __enter__(self) -> SocketFactory:
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class SocketFactoryContextManagerFactory(Protocol):

    def create(self) -> SocketFactoryContextManager:
        pass
