import asyncio

HOST = "localhost"
PORT = 6379


async def handler(reader, writer):
    writer.write("+PONG\r\n".encode())
    await writer.drain()


async def main():
    server = await asyncio.start_server(handler, host=HOST, port=PORT)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
