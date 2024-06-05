import socket
import threading

HOST = "localhost"
PORT = 6379
CRLF = "\r\n"
PONG = f"+PONG{CRLF}"

REDIS_SIG = f"*1{CRLF}$4{CRLF}"
PING = f"{REDIS_SIG}PING{CRLF}"


def req_handler(conn: socket.socket):
    data = conn.recv(512)
    if not data:
        print("No data received")
        return
    print(f"{data=}")
    conn.sendall(b"+PONG\r\n")


def main():
    server_socket = socket.create_server((HOST, PORT), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()  # wait for client
        threading.Thread(target=req_handler, args=(conn,)).start()


if __name__ == "__main__":
    main()
