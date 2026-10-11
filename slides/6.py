import selectors
import time

IoEventType = int
IoEvents = list[tuple[selectors.SelectorKey, IoEventType]]

class Loop:
    _selector: selectors.BaseSelector
    _timers: list
    _ready: list

    def _sleep_until_next_timer_or_io_event(self) -> IoEvents:
        if self._ready:
            delay = 0
        elif self._timers:
            delay = max(0, self._timers[0][0] - time.monotonic())
        else:
            delay = 60

        return self._selector.select(delay) # Instead of time.sleep(delay)

