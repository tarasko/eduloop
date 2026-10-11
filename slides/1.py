class Loop:
    _ready: list

    def call_soon(self, callback, *args):
        self._ready.append((callback, args))

    def run_forever(self):
        while True:
            self._run_once()

    def _run_once(self):
        self._execute_ready_callbacks()

    def _execute_ready_callbacks(self):
        ready = self._ready[:]
        self._ready.clear()

        for callback, args in ready:
            callback(*args)

