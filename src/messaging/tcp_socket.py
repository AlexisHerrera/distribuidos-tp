from socket import AF_INET, SOCK_STREAM, socket


class TCPSocket:
    def __init__(self, port: int):
        self.socket = socket(AF_INET, SOCK_STREAM)
        self.socket.bind(('', port))

    def sendto(self, addr: tuple, message: bytes, bytes_amount: int):
        total_bytes_sent = 0
        while total_bytes_sent < bytes_amount:
            bytes_sent = self.socket.sendto(
                message[total_bytes_sent:bytes_amount], addr
            )
            total_bytes_sent += bytes_sent

    def recvfrom(self, bytes_amount: int):
        message = b''
        host = None
        total_bytes_amount = 0
        while total_bytes_amount < bytes_amount:
            (msg_bytes, addr) = self.socket.recvfrom(bytes_amount - total_bytes_amount)
            msg_bytes_len = len(msg_bytes)
            if msg_bytes_len == 0:
                return None, None
            total_bytes_amount += msg_bytes_len
            message += msg_bytes
            host = addr

        return message, host

    def stop(self):
        self.socket.close()
