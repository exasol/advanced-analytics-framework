import time
from pathlib import Path
from typing import Dict, List

import structlog
from structlog import WriteLoggerFactory
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import key_for_peer
from tests.udf_communication.peer_communication import add_peer_run
from tests.udf_communication.peer_communication.utils import TestProcess

structlog.configure(
    context_class=dict,
    logger_factory=WriteLoggerFactory(file=Path(__file__).with_suffix(".log").open("wt")),
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
    logger = LOGGER.bind(group=group, location="test")
    logger.info("start")
    number_of_instances = 10
    processes: Dict[int, TestProcess] = {}
    connection_infos: Dict[int, ConnectionInfo] = {}
    for i in range(number_of_instances):
        processes[i] = TestProcess(f"t{i}", group, number_of_instances, run=add_peer_run.run)
        processes[i].start()
        connection_infos[i] = processes[i].get()

    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)

    timeout_in_ns = 120 * 10 ** 9
    start_time_ns = time.monotonic_ns()
    while True:
        all_process_stopped = all(not processes[i].is_alive() for i in range(number_of_instances))
        if all_process_stopped:
            break
        difference_ns = time.monotonic_ns() - start_time_ns
        if difference_ns > timeout_in_ns:
            break
        time.sleep(0.01)
    alive_processes_assert = [processes[i].name for i in range(number_of_instances) if processes[i].is_alive()]
    if len(alive_processes_assert) > 0:
        logger.info("failed", processes=alive_processes_assert, reason=f"Processes didn't finish")
    for i in range(number_of_instances):
        process = processes[i]
        if process.is_alive():
            t = process.kill()
    alive_processes = [processes[i].name for i in range(number_of_instances) if processes[i].is_alive()]
    if len(alive_processes) > 0:
        time.sleep(2)
    for i in range(number_of_instances):
        process = processes[i]
        if process.is_alive():
            t = process.terminate()
    assert alive_processes_assert == []

    peers_of_threads: Dict[int, List[ConnectionInfo]] = {}
    for i in range(number_of_instances):
        peers_of_threads[i] = processes[i].get()

    expected_peers_of_threads = {
        i: sorted([
            Peer(connection_info=connection_info)
            for index, connection_info in connection_infos.items()
            if index != i
        ], key=key_for_peer)
        for i in range(number_of_instances)
    }
    if not expected_peers_of_threads == peers_of_threads:
        logger.info("failed", reason=f"Did not get expected_peers_of_threads",
                    expected_peers_of_threads=expected_peers_of_threads,
                    peers_of_threads=peers_of_threads)

    assert expected_peers_of_threads == peers_of_threads

    logger.info("success")
