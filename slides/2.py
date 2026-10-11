import heapq, time

class Loop:
    _timers: list

    def call_later(self, delay, callback, *args):
        when = time.monotonic() + delay
        item = when, callback, args

        heapq.heappush(self._timers, item)

    def run_forever(self): ...
    def _move_expired_timers_to_ready(self): ...
    def _sleep_until_next_timer(self): ...
    def _execute_ready_callbacks(self): ...
