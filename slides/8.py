class Future:
    _done: bool

    def __await__(self):
        if not self._done:
            yield self

        return self.result()

    def result(self) -> Any: ...

async def my_coroutine():
    print("started")
    future = Future()
    await future
    return 42

coro = my_coroutine()    # create coroutine object
try:
    coro.send(None) # start -> print “started” -> yield
    coro.send(None) # resume -> raises StopIteration on return
except StopIteration as ex:
    print(ex.value) # prints "42"