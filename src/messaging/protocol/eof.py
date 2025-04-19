from src.messaging.protocol.message_protocol import MessageProtocol


class EOFProtocol(MessageProtocol):
    def __init__(self):
        pass

    def to_bytes(self, _items) -> tuple[bytes, int]:
        return b'', 0

    def from_bytes(self, _buf: bytes, _bytes_amount: int):
        return None
