from src.messaging.protocol.list_decoder import ListDecoder
from src.messaging.protocol.list_encoder import ListEncoder


class MessageProtocol:
    def __init__(self, item_to_bytes, encode_all, bytes_to_item, decode_all):
        self.__encoder = ListEncoder(item_to_bytes=item_to_bytes, encode_all=encode_all)
        self.__decoder = ListDecoder(bytes_to_item=bytes_to_item, decode_all=decode_all)

    def to_bytes(self, items) -> tuple[bytes, int]:
        return self.__encoder.to_bytes(items)

    def from_bytes(self, buf: bytes, bytes_amount: int) -> list:
        return self.__decoder.from_bytes(buf, bytes_amount)
