class Future:
    def __await__(self):
        print("Future.__await__")
        yield self


async def conversation():
    print("started")
    future = Future()
    await future
    await future
    return 42

coro = conversation()

try:
    # starts coroutine, print “started” then yield
    coro.send(None)
    print("first coro.send done")

    # resume coroutine,
    coro.send(None)
    print("second coro.send done")

    # raises StopIteration because coroutine reaches return
    coro.send(None)
except StopIteration as ex:
    print(ex.value) # prints "42"

