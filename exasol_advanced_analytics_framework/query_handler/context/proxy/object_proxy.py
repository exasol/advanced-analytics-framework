import abc


class ObjectProxy(abc.ABC):
    def __init__(self):
        self._is_valid = True

    def _check_if_valid(self):
        if not self._is_valid:
            raise RuntimeError(f"{self} already released.")

    def _invalidate(self):
        self._check_if_valid()
        self._is_valid = False
