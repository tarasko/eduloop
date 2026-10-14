class Task(Future):
    def __init__(self, coro, loop):
        self.coro = coro
        self.loop = loop
        loop.call_soon(self._step)

    def _step(self, fut=None):
        try:
            if fut is None or fut.exception() is None:
                future = self.coro.send(None)
            else:
                future = self.coro.throw(fut.exception())
        except StopIteration as exc:
            super().set_result(exc.value)
        except BaseException as exc:
            super().set_exception(exc)
        else:
            future.add_done_callback(self._step)


class Future: ...
