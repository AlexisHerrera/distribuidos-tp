import socket


class TCPSocket:
    def __init__(self, socket: socket):
        self.socket: socket = socket

    @classmethod
    def create(cls):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return cls(s)

    @classmethod
    def create_and_connect(cls, addr: tuple):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr)

        return cls(s)

    def accept(self):
        return self.socket.accept()

    def bind(self, addr):
        self.socket.bind(addr)

    def listen(self, n: int):
        self.socket.listen(n)

    def send(self, message: bytes, bytes_amount: int):
        total_bytes_sent = 0
        while total_bytes_sent < bytes_amount:
            bytes_sent = self.socket.send(message[total_bytes_sent:bytes_amount])
            total_bytes_sent += bytes_sent

    def recv(self, bytes_amount: int) -> bytes:
        message = b''
        total_bytes_amount = 0
        while total_bytes_amount < bytes_amount:
            msg_bytes = self.socket.recv(bytes_amount - total_bytes_amount)
            msg_bytes_len = len(msg_bytes)
            if msg_bytes_len == 0:
                return None
            total_bytes_amount += msg_bytes_len
            message += msg_bytes

        return message

    def stop(self):
        if self.socket:
            self.socket.close()
