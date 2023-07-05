from structlog import DropEvent


class ConditionalMethodDropper:
    def __init__(self, method_name):
        self._method_name = method_name

    def __call__(self, logger, method_name, event_dict):
        if method_name == self._method_name:
            raise DropEvent

        return event_dict
