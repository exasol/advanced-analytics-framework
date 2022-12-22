import time


class Clock():
    def get_current_timestamp_in_ms(self) -> int:
        current_timestamp_in_ms = time.monotonic_ns() // 10 ** 6
        return current_timestamp_in_ms
