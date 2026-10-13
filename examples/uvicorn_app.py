import asyncio

import uvicorn

from eduloop import Loop


async def app(scope, receive, send):
    assert scope["type"] == "http"

    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain")],
        }
    )
    await send({"type": "http.response.body", "body": b"Hello from eduloop!\n"})


async def main():
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=8000,
        loop="none",
        lifespan="off",
    )
    await uvicorn.Server(config).serve()


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=Loop)
