import asyncio
from eduloop import Loop


async def hello():
    loop = asyncio.get_running_loop()

    print(f"{loop.time()}: entered hello() coroutine, type(loop)={type(loop)}")
    for i in range(5):
        res = await asyncio.sleep(2.0, "qqq")
        print(f"{asyncio.get_running_loop().time()}: {i}: async sleep hello4: {res}")


if __name__ == "__main__":
    asyncio.run(hello(), loop_factory=Loop)
