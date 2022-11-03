import multiprocessing
import time
from multiprocessing import Process
from queue import Queue
from typing import Any, Callable, List


class BidirectionalQueue:

    def __init__(self, put_queue: Queue, get_queue: Queue):
        self._put_queue = put_queue
        self._get_queue = get_queue

    def put(self, obj: Any):
        self._put_queue.put(obj)

    def get(self) -> Any:
        return self._get_queue.get()


class TestProcess:
    def __init__(self, name: str, group: str, number_of_instances: int,
                 run: Callable[[str, str, int, BidirectionalQueue], None]):
        self.name = name
        put_queue = multiprocessing.Queue()
        get_queue = multiprocessing.Queue()
        self._main_thread_queue = BidirectionalQueue(put_queue=get_queue, get_queue=put_queue)
        thread_queue = BidirectionalQueue(put_queue=put_queue, get_queue=get_queue)
        self._process = Process(target=run, args=(name, group, number_of_instances, thread_queue))

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


def assert_processes_finish(processes: List[TestProcess], timeout_in_seconds: int):
    timeout_in_ns = timeout_in_seconds * 10 ** 9
    start_time_ns = time.monotonic_ns()
    while True:
        no_alive_processes = not any(get_alive_processes(processes))
        if no_alive_processes:
            break
        difference_ns = time.monotonic_ns() - start_time_ns
        if difference_ns > timeout_in_ns:
            break
        time.sleep(0.001)
    alive_processes_before_kill = [process.name for process in get_alive_processes(processes)]
    kill_alive_processes(processes)
    if len(get_alive_processes(processes)) > 0:
        time.sleep(2)
    terminate_alive_processes(processes)
    assert alive_processes_before_kill == []


def terminate_alive_processes(processes: List[TestProcess]):
    for process in get_alive_processes(processes):
        t = process.terminate()


def kill_alive_processes(processes: List[TestProcess]):
    for process in get_alive_processes(processes):
        t = process.kill()


def get_alive_processes(processes: List[TestProcess]) -> List[TestProcess]:
    return [process for process in processes if process.is_alive()]
