import heapq, time

class Loop:
    _ready: list
    _timers: list

    def _run_once(self):
        self._sleep_until_next_timer()
        self._move_expired_timers_to_ready()
        self._execute_ready_callbacks()

    def _sleep_until_next_timer(self):
        if self._ready:
            delay = 0
        elif self._timers:
            delay = max(0, self._timers[0][0] - time.monotonic())
        else:
            delay = 60
        time.sleep(delay)

    def _move_expired_timers_to_ready(self):
        now = time.monotonic()
        while self._timers and self._timers[0][0] <= now:
            _, cb, args = heapq.heappop(self._timers)
            self._ready.append((cb, args))

    def _execute_ready_callbacks(self): ...
