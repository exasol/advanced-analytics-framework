from typing import Protocol

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.communicator import Communicator
from exasol_advanced_analytics_framework.udf_communication.udf_communicator import udf_communicator

LOGGER: FilteringBoundLogger = structlog.get_logger()


class UDFCommunicatorFactory(Protocol):

    def create(self) -> Communicator:
        pass


class DistributedUDF(Protocol):
    def run(self, ctx, exa, udf_communicator_factory: UDFCommunicatorFactory):
        pass


class _RunnerUDFCommunicatorFactory:
    def __init__(self, exa, connection_name: str):
        self._connection_name = connection_name
        self._exa = exa

    def create(self) -> Communicator:
        return udf_communicator(self._exa, self._connection_name)


class DistributedUDFRunner:

    def __init__(self, distributed_udf: DistributedUDF):
        self._distributed_udf = distributed_udf

    def run(self, ctx, exa, connection_name: str):
        factory = _RunnerUDFCommunicatorFactory(exa=exa, connection_name=connection_name)
        self._distributed_udf.run(ctx, exa, factory)
