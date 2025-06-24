from src.messaging.tcp_socket import TCPSocket

DEFAULT_BACKLOG = 1


class ServerSocket:
    def __init__(
        self, port: int, backlog: int = DEFAULT_BACKLOG, timeout: int | None = None
    ):
        self.socket = TCPSocket.create(timeout)
        self.socket.bind(('', port))
        self.socket.listen(backlog)

    def accept(self):
        s, addr = self.socket.accept()

        return TCPSocket(s), addr

    def stop(self):
        self.socket.stop()
