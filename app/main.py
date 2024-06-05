import asyncio

HOST = "localhost"
PORT = 6379
CRLF = "\r\n"
PONG = f"+PONG{CRLF}"

RESP_BULK_SIG = f"$3{CRLF}"

REDIS_PING_SIG = f"*1{CRLF}$4{CRLF}"
PING = f"{REDIS_PING_SIG}PING{CRLF}"
REDIS_ECHO_SIG = f"*2{CRLF}$4{CRLF}ECHO{CRLF}"


def ping() -> bytes:
    return PONG.encode("utf8")

def echo(request_str: str) -> bytes:
    return request_str.replace(REDIS_ECHO_SIG, "").encode("utf8")

async def handle_client(reader, writer):
    request = ""
    while request != 'quit':
        request: str = (await reader.read(255)).decode('utf8')
        print(f"{request=}")
        if not request:
            print("No data received")
            return

        if request.startswith(REDIS_PING_SIG):
            writer.write(ping())
        elif request.startswith(REDIS_ECHO_SIG):
            writer.write(echo(request))
        else:
            return
        await writer.drain()
    writer.close()

async def run_server():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(run_server())
