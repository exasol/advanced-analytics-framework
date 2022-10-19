import time
import traceback
from queue import Queue
from typing import Dict, Any, Callable

import structlog
from structlog.types import FilteringBoundLogger

LOGGER: FilteringBoundLogger = structlog.getLogger(module=__name__)


class LazyValue():
    def __init__(self, callable: Callable):
        self.callable = callable

    def materialize(self) -> Any:
        return self.callable()


def materialize_value(value: Any):
    if isinstance(value, LazyValue):
        return value.materialize()
    else:
        return value


class LoggerThread():

    def __init__(self):
        self._queue: Queue[Dict] = Queue()

    def log(self, event: str, **kw):
        event = dict(event=event, timestamp_ns=time.monotonic_ns())
        log_record = {**event, **kw, }
        self._queue.put(log_record)

    def print(self):
        logger: FilteringBoundLogger = structlog.getLogger()
        try:
            while not self._queue.empty():
                log_record = self._queue.get()
                materialized_log_record = {key: materialize_value(value) for key, value in log_record.items()}
                logger.info(**materialized_log_record)
        except Exception as e:
            LOGGER.exception("Exception",
                             exception=e,
                             stacktrace=traceback.format_exc())
