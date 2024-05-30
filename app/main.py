import asyncio
from asyncio.streams import StreamReader, StreamWriter

HOST = "localhost"
PORT = 6379
CRLF = "\r\n"
PONG = f"+PONG{CRLF}"

REDIS_SIG = f"*1{CRLF}$4{CRLF}"
PING = f"{REDIS_SIG}PING{CRLF}"


async def handler(reader: StreamReader, writer: StreamWriter):
    response = ""
    request = await reader.read(1024)
    print(request)
    if PING in request.decode():
        print("ping in request")
        writer.write(PONG.encode())

    await writer.drain()


async def main():
    server = await asyncio.start_server(handler, host=HOST, port=PORT)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
