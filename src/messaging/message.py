from enum import Enum

from src.dto.movie import MovieProtocol


class MessageType(Enum):
    Unknown = 0
    Movie = 1
    EOF = 100

    @classmethod
    def _missing_(cls, _value):
        return MessageType.Unknown


class Message():
    MSG_TYPE_LEN = 1
    MSG_LEN_SIZE = 2

    def __init__(self, message_type: MessageType, data: object):
        self.message_type = message_type
        self.data = data

    def to_bytes(self) -> bytes:
        data_encoded = b''
        bytes_amount = 0

        match self.message_type:
            case MessageType.Movie:
                (data_encoded, bytes_amount) = MovieProtocol.to_bytes(self.data)
            case MessageType.EOF:
                (data_encoded, bytes_amount) = (b'', 0)

        msg_type_encoded = Message.__int_to_bytes(self.message_type.value, Message.MSG_TYPE_LEN)
        bytes_amount_encoded = Message.__int_to_bytes(bytes_amount, Message.MSG_LEN_SIZE)

        return msg_type_encoded + bytes_amount_encoded + data_encoded

    @staticmethod
    def from_bytes(buf: bytes):
        msg_type_from_buf = Message.__int_from_bytes(buf, Message.MSG_TYPE_LEN)
        bytes_amount = Message.__int_from_bytes(buf[Message.MSG_TYPE_LEN:], Message.MSG_LEN_SIZE)

        decoder = lambda _buf, _bytes_amount: None # pylint: disable=unnecessary-lambda-assignment
        msg_type = MessageType(msg_type_from_buf)

        match msg_type:
            case MessageType.Movie:
                decoder = MovieProtocol.from_bytes
            case MessageType.EOF:
                pass
            case _:
                msg_type = MessageType.Unknown

        data = decoder(buf[Message.MSG_TYPE_LEN + Message.MSG_LEN_SIZE:], bytes_amount)

        return Message(msg_type, data)

    @staticmethod
    def __int_from_bytes(buf: bytes, to: int) -> int:
        return int.from_bytes(buf[0:to], 'big', signed=False)

    @staticmethod
    def __int_to_bytes(value: int, bytes_amount: int) -> bytes:
        return value.to_bytes(bytes_amount, 'big', signed=False)
