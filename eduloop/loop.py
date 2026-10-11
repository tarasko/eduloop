import asyncio
import contextvars
import heapq
import selectors
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from logging import getLogger, Logger
from typing import TypeAlias, Coroutine, Any, Optional

import aiofastnet
import aiodns

Callback: TypeAlias = Callable[..., None]
CallbackArgs: TypeAlias = tuple[Any]
IoEventType = int
IoEvents = list[tuple[selectors.SelectorKey, IoEventType]]


class Handle:
    _loop: asyncio.AbstractEventLoop
    _callback: Callback
    _args: CallbackArgs
    _context: contextvars.Context
    _cancelled: bool

    def __init__(self, loop, callback, args, context: Optional[contextvars.Context] = None):
        self._loop = loop
        self._callback = callback
        self._args = args
        self._context = context if context is not None else contextvars.copy_context()
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def cancelled(self):
        return self._cancelled

    def _run(self):
        if not self._cancelled:
            try:
                self._context.run(self._callback, *self._args)
            except Exception as exc:
                context = {
                    'message': 'user handler raised exception',
                    'exception': exc,
                    'handle': self
                }
                self._loop.call_exception_handler(context)


class TimerHandle(Handle):
    _when: float

    def __init__(self, loop, when, callback, args, context: Optional[contextvars.Context] = None):
        super().__init__(loop, callback, args, context)
        self._when = when

    def when(self):
        return self._when

    def __lt__(self, other):
        if not isinstance(other, TimerHandle):
            raise NotImplementedError
        return self._when < other._when


class FutureState(IntEnum):
    PENDING = 0
    CANCELLED = 1
    FINISHED = 2


class Future:
    _asyncio_future_blocking = False # required by asyncio.isfuture

    _loop: asyncio.AbstractEventLoop
    _state: FutureState = FutureState.PENDING
    _result: Any = None
    _exc: Optional[BaseException] = None
    _callbacks: list[Callback]

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._callbacks = []

    def __await__(self):
        if self._state == FutureState.PENDING:
            yield self

        return self.result()

    def result(self):
        if self._state == FutureState.PENDING:
            raise asyncio.InvalidStateError()

        if self._exc is not None:
            raise self._exc
        else:
            return self._result

    def set_result(self, result):
        if self._state != FutureState.PENDING:
            raise asyncio.InvalidStateError()

        self._result = result
        self._state = FutureState.FINISHED
        self._schedule_callbacks()

    def set_exception(self, exc):
        if self._state != FutureState.PENDING:
            raise asyncio.InvalidStateError()

        self._exc = exc
        self._state = FutureState.FINISHED
        self._schedule_callbacks()

    def done(self):
        return self._state != FutureState.PENDING

    def cancelled(self):
        return self._state == FutureState.CANCELLED

    def add_done_callback(self, callback: Callback):
        if self._state != FutureState.PENDING:
            self._loop.call_soon(callback, self)
        else:
            self._callbacks.append(callback)

    def cancel(self, msg=None):
        if self._state != FutureState.PENDING:
            return False

        self._exc = asyncio.CancelledError(msg)
        self._state = FutureState.CANCELLED
        self._schedule_callbacks()
        return True

    def exception(self):
        if self._state == FutureState.CANCELLED:
            raise self._exc
        if self._state == FutureState.PENDING:
            raise asyncio.InvalidStateError()

        return self._exc

    def get_loop(self):
        return self._loop

    def _schedule_callbacks(self):
        for c in self._callbacks:
            self._loop.call_soon(c, self)
        self._callbacks.clear()


class Task(Future):
    _coro: Any
    _must_cancel: bool = False

    def __init__(self, coro, loop):
        super().__init__(loop)
        self._coro = coro

        loop.call_soon(self._step)

    def set_result(self, result):
        raise RuntimeError('Task does not support set_result operation')

    def set_exception(self, exc):
        raise RuntimeError('Task does not support set_exception operation')

    def cancel(self, msg=None):
        self._must_cancel = True
        self._loop.call_soon(self._step) # _step will emit CancelledError into the coroutine

    def _step(self, exc=None):
        if self.done():  # This can happen if we have both cancellation and wake up upon future completion
            return

        try:
            if self._must_cancel: # Keep emitting CancelledError() for simplicity, asyncio has a counter here instead
                fut = self._coro.throw(asyncio.CancelledError())
            elif exc is None:
                fut = self._coro.send(None)
            else:
                fut = self._coro.throw(exc)
        except StopIteration as ex:
            super().set_result(ex.value)
        except asyncio.CancelledError:
            super().cancel()
        except BaseException as ex:
            super().set_exception(ex)
        else:
            # fut can be None, this is an internal optimization in asyncio.sleep(0) to just skip one loop iteration
            # without creating Future object.
            if fut is not None:
                fut.add_done_callback(self._wakeup)
            else:
                self._loop.call_soon(self._step)

    def _wakeup(self, future):
        try:
            future.result()
            self._step()
        except BaseException as exc:
            self._step(exc)


class Loop(asyncio.AbstractEventLoop):
    _running: bool = False
    _stopping: bool = False
    _debug: bool = False
    _logger: Logger
    _ready: list[Handle]
    _timers: list[TimerHandle]
    _selector: selectors.BaseSelector
    _dns_resolver: aiodns.DNSResolver

    connect_accepted_socket = aiofastnet.connect_accepted_socket
    connect_read_pipe = aiofastnet.connect_read_pipe
    connect_write_pipe = aiofastnet.connect_write_pipe
    create_connection = aiofastnet.create_connection
    create_datagram_endpoint = aiofastnet.create_datagram_endpoint
    create_server = aiofastnet.create_server
    create_unix_connection = aiofastnet.create_unix_connection
    create_unix_server = aiofastnet.create_unix_server
    sendfile = aiofastnet.sendfile
    start_tls = aiofastnet.start_tls

    sock_accept = aiofastnet.sock_accept
    sock_connect = aiofastnet.sock_connect
    sock_recv = aiofastnet.sock_recv
    sock_recv_into = aiofastnet.sock_recv_into
    sock_recvfrom = aiofastnet.sock_recvfrom
    sock_recvfrom_into = aiofastnet.sock_recvfrom_into
    sock_sendall = aiofastnet.sock_sendall
    sock_sendto = aiofastnet.sock_sendto

    def __init__(self):
        self._logger = getLogger("eduloop")
        self._ready = []
        self._timers = []
        self._selector = selectors.DefaultSelector()
        self._dns_resolver = aiodns.DNSResolver(loop=self)

    def time(self):
        return time.monotonic()

    def is_running(self) -> bool:
        return self._running

    def stop(self):
        self._stopping = True

    def call_soon(self, callback: Callback, *args, context=None) -> Handle:
        h = Handle(self, callback, args, context)
        self._ready.append(h)
        return h

    def call_at(self, when: float, callback: Callback, *args, context=None) -> TimerHandle:
        h = TimerHandle(self, when, callback, args, context)
        heapq.heappush(self._timers, h)
        return h

    def call_later(self, delay: float, callback: Callback, *args, context=None):
        when = self.time() + delay
        return self.call_at(when, callback, *args, context=context)

    def call_soon_threadsafe(self, callback: Callback, *args, context=None) -> Handle:
        # TODO: This is not a proper implementation, but good enough for demo
        h = Handle(self, callback, args, context)
        self._ready.append(h)
        return h

    def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        return self._dns_resolver.getaddrinfo(host, family, port, proto, type, flags)

    def getnameinfo(self, sockaddr, flags=0):
        return self._dns_resolver.getnameinfo(sockaddr, flags)

    def create_task(self, coro: Coroutine, **kwargs) -> Task:
        return Task(coro, loop=self)

    def create_future(self) -> Future:
        return Future(loop=self)

    @dataclass(repr=False, eq=False)
    class _FdData:
        read_ready_handle: Optional[Handle] = None
        write_ready_handle: Optional[Handle] = None

    def add_reader(self, fd, callback, *args) -> None:
        self._add_or_update_fd_ready_handle(
            fd, Handle(self, callback, args), selectors.EVENT_READ, "read_ready_handle")

    def add_writer(self, fd, callback, *args) -> None:
        self._add_or_update_fd_ready_handle(
            fd, Handle(self, callback, args), selectors.EVENT_WRITE, "write_ready_handle")

    def remove_reader(self, fd) -> bool:
        return self._remove_fd_ready_handle(fd, selectors.EVENT_READ, "read_ready_handle")

    def remove_writer(self, fd) -> bool:
        return self._remove_fd_ready_handle(fd, selectors.EVENT_WRITE, "write_ready_handle")

    def call_exception_handler(self, context):
        self._logger.error("Unhandled exception: %s", context)

    def get_debug(self):
        return self._debug

    def set_debug(self, enabled):
        self._debug = enabled

    async def shutdown_asyncgens(self):
        # TODO: Our loop does not currently track running async generators. Feel free to add this
        return

    async def shutdown_default_executor(self, timeout=None):
        return

    def close(self):
        self._selector.close()

    def run_forever(self):
        self._running = True
        asyncio._set_running_loop(self)
        try:
            while not self._stopping:
                self._run_once()
        finally:
            self._running = False

    def run_until_complete(self, future):
        self._running = True
        asyncio._set_running_loop(self)
        task = asyncio.ensure_future(future, loop=self)
        try:
            while not self._stopping:
                self._run_once()
                if task.done():
                    return task.result()
        finally:
            self._running = False

    def _run_once(self) -> None:
        events = self._sleep_until_next_timer_or_io_event()
        self._move_io_events_to_ready(events)
        self._move_expired_timers_to_ready()
        self._execute_ready_callbacks()

    def _move_io_events_to_ready(self, events):
        for key, event in events:
            if event & selectors.EVENT_READ:
                self._ready.append(key.data.read_ready_handle)
            if event & selectors.EVENT_WRITE:
                self._ready.append(key.data.write_ready_handle)

    def _move_expired_timers_to_ready(self):
        now = self.time()
        while self._timers and self._timers[0].when() <= now:
            item = heapq.heappop(self._timers)
            self._ready.append(item)

    def _execute_ready_callbacks(self):
        callbacks = self._ready[:]
        self._ready.clear()
        for h in callbacks:
            try:
                h._run()
            except Exception as exc:
                self._logger.exception(f"Exception in callback {h}")

    def _sleep_until_next_timer_or_io_event(self) -> IoEvents:
        if self._ready:
            delay = 0
        elif self._timers:
            delay = max(0, self._timers[0].when() - time.monotonic())
        else:
            delay = 60

        return self._selector.select(delay)

    def _add_or_update_fd_ready_handle(self, fd, handle: Handle, event: int, attr_name: str):
        mapping = self._selector.get_map()
        key = mapping.get(fd)
        if key is None:
            data = Loop._FdData()
            setattr(data, attr_name, handle)
            self._selector.register(fd, event, data)
        else:
            setattr(key.data, attr_name, handle)
            if key.events & event == 0:
                self._selector.modify(fd, key.events | event)

    def _remove_fd_ready_handle(self, fd, event: int, attr_name: str) -> bool:
        mapping = self._selector.get_map()
        key = mapping.get(fd)
        if key is None or key.events & event == 0:
            return False

        if key.events ^ event == 0:
            self._selector.unregister(fd)
        else:
            setattr(key.data, attr_name, None)
            self._selector.modify(fd, event)

        return True
