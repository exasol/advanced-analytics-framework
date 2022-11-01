import time
from typing import Dict, Set, List

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from tests.udf_communication.peer_communication import send_recv_run
from tests.udf_communication.peer_communication.utils import TestThread


def test():
    group = f"{time.monotonic_ns()}"
    number_of_instances = 10
    threads: Dict[int, TestThread] = {}
    connection_infos: Dict[int, ConnectionInfo] = {}
    for i in range(number_of_instances):
        threads[i] = TestThread(f"t{i}", group, number_of_instances, run=send_recv_run.run)
        threads[i].start()
        connection_infos[i] = threads[i].get()

    for i in range(number_of_instances):
        t = threads[i].put(connection_infos)

    for i in range(number_of_instances):
        threads[i].get()

    received_values: Dict[int, Set[str]] = {}
    for i in range(number_of_instances):
        received_values[i] = threads[i].get()

    expected_received_values = {
        i: {
            thread.name
            for index, thread in threads.items()
            if index != i
        }
        for i in range(number_of_instances)
    }
    assert expected_received_values == received_values

    for i in range(number_of_instances):
        threads[i].join(timeout=10)
        assert not threads[i].is_alive()
