import selectors
import time

class Loop:
    _selector: selectors.BaseSelector
    _timers: list
    _ready: list

    def _sleep_until_next_timer(self) -> None:
        if self._ready:
            delay = 0
        elif self._timers:
            delay = max(0, self._timers[0][0] - time.monotonic())
        else:
            delay = 60

        time.sleep(delay)

