class Future:
    _done: bool

    def result(self) -> Any: ...

    def __await__(self):
        if not self._done:
            yield self

        return self.result()

async def conversation():
    print("started")
    future = Future()
    await future
    return 42

coro = conversation()    # create coroutine object
future = coro.send(None) # start -> print “started” -> yield

try:
    # resume -> raises StopIteration on return
    coro.send(None)
except StopIteration as ex:
    print(ex.value) # prints "42"