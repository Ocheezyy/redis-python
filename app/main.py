import asyncio
from asyncio.streams import StreamWriter

HOST = "localhost"
PORT = 6379
CRLF = "\r\n"
PONG = f"+PONG{CRLF}"


def redis_ping() -> bytes:
    return redis_encode("PONG")


def redis_echo(req_arr: list[str]) -> bytes:
    return redis_encode([el for el in req_arr[4:5]])


def redis_encode(data, encoding="utf-8"):
    if not isinstance(data, list):
        data = [data]
    size = len(data)
    encoded = []
    for datum in data:
        encoded.append(f"${len(datum)}")
        encoded.append(datum)
    if size > 1:
        encoded.insert(0, f"*{size}")
    print(f"encoded: {encoded}")
    return (CRLF.join(encoded) + CRLF).encode(encoding=encoding)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    request = ""
    while request != 'quit':
        request: str = (await reader.read(255)).decode()
        print(f"{request=}")
        if not request:
            # print("No data received")
            return

        req_arr: list[str] = request.split(CRLF)
        req_arr_size = len(req_arr)
        print(f"req_arr: ${req_arr}")

        response: bytes
        if req_arr[2].lower() == "ping":
            response = redis_ping()
        elif req_arr[2].lower() == "echo":
            response = redis_echo(req_arr)
        else:
            return

        print(f"response: {response}")
        writer.write(response)
        await writer.drain()
    writer.close()


async def run_server():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(run_server())
