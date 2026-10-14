import selectors

IoEventType = int
IoEvents = list[tuple[selectors.SelectorKey, IoEventType]]

class Loop:
    _selector: selectors.BaseSelector
    _timers: list
    _ready: list

    def _run_once(self):
        io_events = self._sleep_until_next_timer_or_io_event()
        self._move_io_events_to_ready(io_events)
        self._move_expired_timers_to_ready()
        self._execute_ready_callbacks()

    def _move_io_events_to_ready(self, io_events: IoEvents):
        for key, event in io_events:
            self._ready.append(key.data[event])

    def _sleep_until_next_timer_or_io_event(self) -> IoEvents: ...
    def _move_expired_timers_to_ready(self): ...
    def _execute_ready_callbacks(self): ...

