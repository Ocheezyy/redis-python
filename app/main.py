import asyncio
import socket
from typing import Any, Optional
from argparse import ArgumentParser, Namespace

HOST = "localhost"
DEFAULT_PORT = 6379
CRLF = "\r\n"
NULL_BULK_STRING = f"$-1{CRLF}"

MASTER_STORE: dict[str, Any] = {}

replication_info: dict[str, Any] = {
    "connected_slaves": 0,
    "second_repl_offset": -1,
    "repl_backlog_active": 0,
    "repl_backlog_size": 1048576,
    "repl_backlog_first_byte_offset": 0,
    "repl_backlog_histlen": "",
}


def get_replication_info_lst():
    global replication_info
    return [f"{k}:{v}" for k, v in replication_info.items()]


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


def redis_info(req_arr: list[str]) -> bytes:
    if len(req_arr) >= 5 and req_arr[4].lower() == "replication":
        return redis_encode(CRLF.join(get_replication_info_lst()))
    return b""


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
        elif req_arr[2].lower() == "info":
            response = redis_info(req_arr)
        elif req_arr[2].lower() == "replconf":
            response = b""
        elif req_arr[2].lower() == "psync":
            response = b""
        else:
            return

        print(f"response: {response}")
        writer.write(response)
        await writer.drain()
    writer.close()


def build_replication_info(input_args: Namespace) -> None:
    global replication_info
    if input_args.replicaof == "master":
        replication_info["role"] = "master"
        # Generate master replid here in future
        replication_info["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        replication_info["master_repl_offset"] = 0
    else:
        master_addr, master_port = input_args.replicaof.split(" ")
        replication_info["role"] = "slave"
        master_handshake(master_addr, int(master_port))


def master_handshake(master_addr: str, master_port: int):
    master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_sock.connect((master_addr, master_port))
    master_sock.send(f"*1{CRLF}$4{CRLF}PING{CRLF}".encode())


async def run_server():
    global replication_info
    parser = ArgumentParser("Redis Python")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--replicaof", type=str, default="master")
    input_args = parser.parse_args()
    build_replication_info(input_args)

    server = await asyncio.start_server(handle_client, HOST, input_args.port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(run_server())
