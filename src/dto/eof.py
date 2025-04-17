class EOFProtocol:
    def __init__(self):
        pass

    def to_bytes(self, _items) -> tuple[bytes, int]:
        return b'', 0
