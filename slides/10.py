class Task(Future):
    def __init__(self, coro, loop):
        self.coro = coro
        self.loop = loop
        loop.call_soon(self.step)

    def step(self):
        try:
            future = self.coro.send(None)
        except StopIteration:
            return

        future.add_done_callback(self.wakeup)

    def wakeup(self, future):
        self.step()


class Future: ...
