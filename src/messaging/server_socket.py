from src.messaging.tcp_socket import TCPSocket


class ServerSocket:
    def __init__(self, port: int, backlog: int = 1):
        self.socket = TCPSocket.create()
        self.socket.bind(('', port))
        self.socket.listen(backlog)

    def accept(self):
        s, addr = self.socket.accept()

        return TCPSocket(s), addr

    def stop(self):
        self.socket.stop()
