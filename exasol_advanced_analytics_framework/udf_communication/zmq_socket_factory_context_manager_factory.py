from typing import Optional

import zmq

from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_wrapper import ZMQSocketFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory_context_manager_factory import \
    SocketFactoryContextManager


class ZMQSocketFactoryContextManager:
    def __init__(self):
        self._context: Optional[zmq.Context] = None

    def __enter__(self) -> SocketFactory:
        self._context = zmq.Context()
        return ZMQSocketFactory(self._context)

    def _close(self):
        if self._context is not None:
            self._context.destroy()
            self._context = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()

    def __del__(self):
        self._close()


class ZMQSocketFactoryContextManagerFactory:

    def create(self) -> SocketFactoryContextManager:
        return ZMQSocketFactoryContextManager()
