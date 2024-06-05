import asyncio
from typing import Any
from argparse import ArgumentParser

HOST = "localhost"
DEFAULT_PORT = 6379
CRLF = "\r\n"
NULL_BULK_STRING = f"$-1{CRLF}"

MASTER_STORE: dict[str, Any] = {}

async def expire_key(store_key: str, exp_time: int):
    await asyncio.sleep(exp_time / 1000)
    MASTER_STORE.pop(store_key)


def redis_ping() -> bytes:
    """Server health check for redis cli (ping -> pong)"""
    return redis_encode("+PONG")


def redis_echo(req_arr: list[str]) -> bytes:
    """Echo a redis cli request"""
    return redis_encode([el for el in req_arr[4:5]])


def redis_set(req_arr: list[str]) -> bytes:
    """Set a KV pair in the redis store"""
    global MASTER_STORE
    req_key: str = req_arr[4]
    req_val: str = req_arr[6]
    MASTER_STORE[req_key] = req_val

    if len(req_arr) > 8 and req_arr[8].lower() == "px":
        exp_time = int(req_arr[10])
        asyncio.create_task(coro=expire_key(req_key, exp_time))
    return redis_encode("+OK")


def redis_get(req_arr: list[str]) -> bytes:
    """Retrieve a value for a key in the redis store"""
    global MASTER_STORE
    req_key = req_arr[4]
    if not req_key or req_key not in MASTER_STORE:
        return NULL_BULK_STRING.encode()
    return redis_encode(MASTER_STORE[req_key])


def redis_encode(data, encoding="utf-8"):
    """Encode a reponse for redis cli"""
    if isinstance(data, str) and data.startswith("+"):
        return f"{data}{CRLF}".encode()
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
    """async request handler for redis requests"""
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
        elif req_arr[2].lower() == "set":
            response = redis_set(req_arr)
        elif req_arr[2].lower() == "get":
            response = redis_get(req_arr)
        else:
            return

        print(f"response: {response}")
        writer.write(response)
        await writer.drain()
    writer.close()


async def run_server():
    parser = ArgumentParser("Redis Python")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    input_args = parser.parse_args()
    server = await asyncio.start_server(handle_client, HOST, input_args.port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(run_server())
