import time


class Clock():
    def get_current_timestamp_in_ms(self) -> int:
        timestamp = time.monotonic_ns() // 10 ** 6
        return timestamp
