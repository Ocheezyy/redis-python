import asyncio

HOST = "localhost"
PORT = 6379
CRLF = "\r\n"
PONG = f"+PONG{CRLF}"

REDIS_SIG = f"*1{CRLF}$4{CRLF}"
PING = f"{REDIS_SIG}PING{CRLF}"


async def handle_client(reader, writer):
    request = None
    while request != 'quit':
        request = (await reader.read(255)).decode('utf8')
        print(f"{request=}")
        if not request:
            print("No data received")
            return
        writer.write(PONG.encode('utf8'))
        await writer.drain()
    writer.close()

async def run_server():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(run_server())
