import multiprocessing
from multiprocessing import Process
from queue import Queue
from threading import Thread
from typing import Any, Callable


class BidirectionalQueue:

    def __init__(self, put_queue: Queue, get_queue: Queue):
        self._put_queue = put_queue
        self._get_queue = get_queue

    def put(self, obj: Any):
        self._put_queue.put(obj)

    def get(self) -> Any:
        return self._get_queue.get()


class TestThread:
    def __init__(self, name: str, group: str, number_of_instances: int,
                 run: Callable[[str, str, int, BidirectionalQueue], None]):
        self.name = name
        put_queue = Queue()
        get_queue = Queue()
        self._main_thread_queue = BidirectionalQueue(put_queue=get_queue, get_queue=put_queue)
        thread_queue = BidirectionalQueue(put_queue=put_queue, get_queue=get_queue)
        self._thread = Thread(target=run, args=(name, group, number_of_instances, thread_queue))

    def start(self):
        self._thread.start()

    def put(self, obj: Any):
        self._main_thread_queue.put(obj)

    def get(self) -> Any:
        return self._main_thread_queue.get()

    def join(self, timeout=None):
        self._thread.join(timeout)

    def is_alive(self):
        return self._thread.is_alive()


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