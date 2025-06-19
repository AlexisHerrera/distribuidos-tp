import logging
import socket

logger = logging.getLogger(__name__)


class TCPSocket:
    def __init__(self, socket: socket):
        self.socket: socket = socket

    @classmethod
    def create(cls):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return cls(s)

    @classmethod
    def create_and_connect(cls, addr: tuple, timeout: int | None = None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if timeout:
            s.settimeout(timeout)

        s.connect(addr)

        return cls(s)

    def accept(self):
        return self.socket.accept()

    def bind(self, addr):
        self.socket.bind(addr)

    def listen(self, backlog: int):
        self.socket.listen(backlog)

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
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError as e:
            logger.error(f'Error while shutting down socket: {e}')
        try:
            self.socket.close()
        except OSError as e:
            logger.error(f'Error while closing socket: {e}')
