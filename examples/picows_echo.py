import asyncio

from picows.websockets import connect
from picows.websockets.asyncio.server import serve

from eduloop import Loop


from picows import ws_create_server, WSFrame, WSTransport, WSListener, \
    WSMsgType, WSUpgradeRequest, WSCloseCode, ws_connect


class ServerClientListener(WSListener):
    def on_ws_connected(self, transport: WSTransport):
        print("New client connected")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        if frame.msg_type == WSMsgType.CLOSE:
            transport.send_close(frame.get_close_code(), frame.get_close_message())
            transport.disconnect()
        else:
            transport.send(frame.msg_type, frame.get_payload_as_memoryview(), frame.fin, frame.rsv1)


async def run_server():
    def listener_factory(r: WSUpgradeRequest):
        # Routing can be implemented here by analyzing request content
        return ServerClientListener()

    server: asyncio.Server = await ws_create_server(listener_factory,
                                                    "127.0.0.1", 9001)
    for s in server.sockets:
        print(f"Server started on {s.getsockname()}")

    await server.serve_forever()


class ClientListener(WSListener):
    def on_ws_connected(self, transport: WSTransport):
        transport.send(WSMsgType.TEXT, b"Hello world")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        print(f"Echo reply: {frame.get_payload_as_ascii_text()}")
        transport.send_close(WSCloseCode.OK)
        transport.disconnect()


async def run_client(url):
    transport, client = await ws_connect(ClientListener, url)
    await transport.wait_disconnected()


async def main():
    srv_task = asyncio.get_running_loop().create_task(run_server())
    await asyncio.sleep(1)
    await run_client("ws://127.0.0.1:9001/ws/echo")


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=Loop)