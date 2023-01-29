from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock


class Timer:

    def __init__(self,
                 clock: Clock,
                 timeout_in_ms: int):
        self._timeout_in_ms = timeout_in_ms
        self._clock = clock
        self._last_send_timestamp_in_ms = clock.current_timestamp_in_ms()

    def reset_timer(self):
        self._last_send_timestamp_in_ms = self._clock.current_timestamp_in_ms()

    def is_time(self):
        current_timestamp_in_ms = self._clock.current_timestamp_in_ms()
        diff = current_timestamp_in_ms - self._last_send_timestamp_in_ms
        return diff > self._timeout_in_ms


class TimerFactory:

    def create(self,
               clock: Clock,
               timeout_in_ms: int):
        return Timer(clock=clock, timeout_in_ms=timeout_in_ms)
