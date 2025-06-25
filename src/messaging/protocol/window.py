from src.messaging.protocol.message_protocol import MessageProtocol


class WindowProtocol(MessageProtocol):
    def __init__(self):
        pass

    def to_bytes(self, data: str) -> tuple[bytes, int]:
        if data is None:
            return b'', 0

        encoded_data = data.encode('utf-8')
        return encoded_data, len(encoded_data)

    def from_bytes(self, data_bytes: bytes, data_len: int) -> str | None:
        if data_len == 0:
            return None

        return data_bytes[:data_len].decode('utf-8')
