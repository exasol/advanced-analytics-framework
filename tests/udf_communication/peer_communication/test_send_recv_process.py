import time
from pathlib import Path
from typing import Dict, Set

import structlog
from structlog import WriteLoggerFactory

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from tests.udf_communication.peer_communication import send_recv_run
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


def test():
    group = f"{time.monotonic_ns()}"
    number_of_instances = 10
    processes: Dict[int, TestProcess] = {}
    connection_infos: Dict[int, ConnectionInfo] = {}
    for i in range(number_of_instances):
        processes[i] = TestProcess(f"t{i}", group, number_of_instances, run=send_recv_run.run)
        processes[i].start()
        connection_infos[i] = processes[i].get()

    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)

    for i in range(number_of_instances):
        processes[i].get()

    received_values: Dict[int, Set[str]] = {}
    for i in range(number_of_instances):
        received_values[i] = processes[i].get()

    expected_received_values = {
        i: {
            thread.name
            for index, thread in processes.items()
            if index != i
        }
        for i in range(number_of_instances)
    }
    assert expected_received_values == received_values

    for i in range(number_of_instances):
        processes[i].join(timeout=5)
        assert not processes[i].is_alive()
