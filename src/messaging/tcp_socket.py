import logging
import socket
import time

logger = logging.getLogger(__name__)

DEFAULT_RETRY_TIMEOUT = 2  # seconds


class SocketDisconnected(Exception):
    "Raised when received 0 bytes from socket"

    def __init__(self):
        super().__init__('Socket disconnected')


class TCPSocket:
    def __init__(self, socket: socket):
        self.socket: socket = socket

    @classmethod
    def create(cls, timeout: int | None = None):
        if timeout:
            socket.setdefaulttimeout(timeout)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return cls(s)

    @classmethod
    def create_and_connect(cls, addr: tuple, timeout: int | None = None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if timeout:
            s.settimeout(timeout)

        s.connect(addr)

        return cls(s)

    @staticmethod
    def connect_while_condition(
        host: str,
        port: int,
        condition: callable,
        retry_timeout: int = DEFAULT_RETRY_TIMEOUT,
        timeout: int | None = None,
    ):
        addr = (host, port)
        socket = None
        while condition():
            try:
                socket = TCPSocket.create_and_connect(addr, timeout)
                break
            except ConnectionRefusedError as e:
                logger.warning(f'Could not connect to {host}:{port}: {e}')
            except OSError as e:
                logger.warning(f'Could not connect to {host}:{port}: {e}')

            time.sleep(retry_timeout)

        return socket

    @staticmethod
    def gethostbyaddress(addr: tuple) -> str:
        (host, _, _) = socket.gethostbyaddr(addr[0])
        return host.split('.')[0]  # Take the first part of the host name

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
                raise SocketDisconnected
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
