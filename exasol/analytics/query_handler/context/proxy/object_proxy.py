import abc


class ObjectProxy(abc.ABC):
    def __init__(self):
        self._not_released = True

    def _check_if_released(self):
        if not self._not_released:
            raise RuntimeError(f"{self} already released.")

    def _release(self):
        self._check_if_released()
        self._not_released = False
