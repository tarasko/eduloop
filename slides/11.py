async def sock_recv(loop, sock, n):
    try:
        return sock.recv(n)
    except BlockingIOError:
        pass

    future = loop.create_future()
    def on_readable():
        data = sock.recv(n)
        loop.remove_reader(sock)
        future.set_result(data)

    loop.add_reader(sock, on_readable)
    return await future

