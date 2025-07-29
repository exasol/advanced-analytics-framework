import json
import multiprocessing
import time
from abc import ABC
from multiprocessing import Process
from queue import Queue
from typing import (
    Any,
    Callable,
    Generic,
    List,
    TypeVar,
)

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication.ip_address import Port

NANOSECONDS_PER_SECOND = 10**9

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class BidirectionalQueue:

    def __init__(self, put_queue: Queue, get_queue: Queue):
        self._put_queue = put_queue
        self._get_queue = get_queue

    def put(self, obj: Any):
        self._put_queue.put(obj)

    def get(self) -> Any:
        return self._get_queue.get()


class TestProcessParameter(ABC):
    __test__ = False

    def __init__(self, seed: int):
        self.seed = seed

    def __repr__(self):
        return json.dumps(self.__dict__)


class PeerCommunicatorTestProcessParameter(TestProcessParameter):
    def __init__(
        self,
        instance_name: str,
        group_identifier: str,
        number_of_instances: int,
        seed: int,
    ):
        super().__init__(seed)
        self.number_of_instances = number_of_instances
        self.group_identifier = group_identifier
        self.instance_name = instance_name


class CommunicatorTestProcessParameter(TestProcessParameter):
    def __init__(
        self,
        node_name: str,
        instance_name: str,
        group_identifier: str,
        number_of_nodes: int,
        number_of_instances_per_node: int,
        local_discovery_port: Port,
        seed: int,
    ):
        super().__init__(seed)
        self.local_discovery_port = local_discovery_port
        self.number_of_instances_per_node = number_of_instances_per_node
        self.number_of_nodes = number_of_nodes
        self.node_name = node_name
        self.group_identifier = group_identifier
        self.instance_name = instance_name


T = TypeVar("T")


class TestProcess(Generic[T]):
    __test__ = False

    def __init__(self, parameter: T, run: Callable[[T, BidirectionalQueue], None]):
        self.parameter = parameter
        put_queue = multiprocessing.Queue()
        get_queue = multiprocessing.Queue()
        self._main_thread_queue = BidirectionalQueue(
            put_queue=get_queue, get_queue=put_queue
        )
        thread_queue = BidirectionalQueue(put_queue=put_queue, get_queue=get_queue)
        self._process = Process(target=run, args=(self.parameter, thread_queue))

    def start(self):
        self._process.start()

    def put(self, obj: Any):
        self._main_thread_queue.put(obj)

    def get(self) -> Any:
        return self._main_thread_queue.get()

    def join(self, timeout=None):
        self._process.join(timeout)

    def is_alive(self):
        return self._process.is_alive()

    def kill(self):
        self._process.kill()

    def terminate(self):
        self._process.terminate()


def assert_processes_finish(processes: list[TestProcess], timeout_in_seconds: int):
    timeout_in_ns = timeout_in_seconds * NANOSECONDS_PER_SECOND
    start_time_ns = time.monotonic_ns()
    while True:
        alive_processes = get_alive_processes(processes)
        no_alive_processes = not any(alive_processes)
        if no_alive_processes:
            break
        difference_ns = time.monotonic_ns() - start_time_ns
        if difference_ns > timeout_in_ns:
            break
        time.sleep(0.01)
    alive_processes_before_kill = [
        process.parameter for process in get_alive_processes(processes)
    ]
    kill_alive_processes(processes)
    if len(get_alive_processes(processes)) > 0:
        time.sleep(2)
    terminate_alive_processes(processes)
    assert alive_processes_before_kill == []


def terminate_alive_processes(processes: list[TestProcess]):
    for process in get_alive_processes(processes):
        t = process.terminate()


def kill_alive_processes(processes: list[TestProcess]):
    for process in get_alive_processes(processes):
        t = process.kill()


def get_alive_processes(processes: list[TestProcess]) -> list[TestProcess]:
    return [process for process in processes if process.is_alive()]
