import socket
HOST = "localhost"
PORT = 6379
CRLF = "\r\n"
PONG = f"+PONG{CRLF}"

REDIS_SIG = f"*1{CRLF}$4{CRLF}"
PING = f"{REDIS_SIG}PING{CRLF}"


def main():
    server_socket = socket.create_server((HOST, PORT), reuse_port=True)
    conn, addr = server_socket.accept()  # wait for client
    with conn:
        # loop
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print(f"{data=}")
            conn.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
