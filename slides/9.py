async def conversation():
    print("started")
    future = Future()
    await future
    return 42

coro = conversation()

# starts coroutine, print “started” then yield
future = coro.send(None)

try:
    # resume coroutine,
    # raises StopIteration on return
    coro.send(None)
except StopIteration as ex:
    print(ex.value) # prints "42"


class Future: ...
