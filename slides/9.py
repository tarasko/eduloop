async def my_coroutine():
    print("started")
    future = Future()
    await future
    return 42

coro = my_coroutine()

# starts coroutine, print “started” then yield
future = coro.send(None)

try:
    # resume coroutine,
    # raises StopIteration on return
    coro.send(None)
except StopIteration as ex:
    print(ex.value) # prints "42"


class Future:
    def __await__(self):
        yield self
