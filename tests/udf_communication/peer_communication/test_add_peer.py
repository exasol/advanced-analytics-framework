import time
from typing import Dict, List

import structlog
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import key_for_peer
from tests.udf_communication.peer_communication import add_peer_run

from tests.udf_communication.peer_communication.utils import TestThread

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(),
        structlog.processors.JSONRenderer()
    ]
)

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


def test():
    group = f"{time.monotonic_ns()}"
    logger = LOGGER.bind(group=group)
    logger.info("TEST START")
    number_of_instances = 10
    threads: Dict[int, TestThread] = {}
    connection_infos: Dict[int, ConnectionInfo] = {}
    for i in range(number_of_instances):
        threads[i] = TestThread(f"t{i}", group, number_of_instances, run=add_peer_run.run)
        threads[i].start()
        connection_infos[i] = threads[i].get()

    for i in range(number_of_instances):
        t = threads[i].put(connection_infos)

    peers_of_threads: Dict[int, List[ConnectionInfo]] = {}
    for i in range(number_of_instances):
        peers_of_threads[i] = threads[i].get()

    expected_peers_of_threads = {
        i: sorted([
            Peer(connection_info=connection_info)
            for index, connection_info in connection_infos.items()
            if index != i
        ], key=key_for_peer)
        for i in range(number_of_instances)
    }
    assert expected_peers_of_threads == peers_of_threads

    for i in range(number_of_instances):
        threads[i].join(timeout=10)
        assert not threads[i].is_alive()
    logger.info("TEST END")