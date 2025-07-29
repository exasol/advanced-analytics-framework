import re


class pytest_regex:
    """Assert that a given string meets some expectations."""

    def __init__(self, pattern, flags=0):
        self._regex = re.compile(pattern, flags)

    def __eq__(self, actual):
        match = self._regex.match(actual)
        return bool(match)

    def __repr__(self):
        return self._regex.pattern
