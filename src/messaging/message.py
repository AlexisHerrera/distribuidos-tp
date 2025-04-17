from enum import Enum

from src.dto.movie import MovieProtocol


class MessageType(Enum):
    Unknown = 0
    Movie = 1
    EOF = 100


class Message():
    MSG_TYPE_LEN = 1
    MSG_LEN_SIZE = 2

    def __init__(self, message_type: MessageType, data: object):
        self.message_type = message_type
        self.data = data

    def to_bytes(self) -> bytes:
        # msg_type = 0
        data_encoded = b''
        bytes_amount = 0

        match self.message_type:
            case MessageType.Movie:
                # msg_type = MessageType.Movie
                (data_encoded, bytes_amount) = MovieProtocol.to_bytes(self.data)
            case MessageType.EOF:
                # msg_type = MessageType.EOF
                (data_encoded, bytes_amount) = (b'', 0)

        msg_type_encoded = self.message_type.value.to_bytes(Message.MSG_TYPE_LEN, 'big')
        # msg_type_encoded = msg_type.value.to_bytes(Message.MSG_TYPE_LEN, 'big')
        bytes_amount_encoded = bytes_amount.to_bytes(Message.MSG_LEN_SIZE, 'big')

        return msg_type_encoded + bytes_amount_encoded + data_encoded

    @staticmethod
    def from_bytes(buf: bytes):
        msg_type_from_buf = Message.__int_from_bytes(buf, Message.MSG_TYPE_LEN)
        bytes_amount = Message.__int_from_bytes(buf[Message.MSG_TYPE_LEN:], Message.MSG_LEN_SIZE)

        decoder = lambda _buf, _bytes_amount: None # pylint: disable=unnecessary-lambda-assignment
        msg_type = MessageType.Unknown

        match MessageType(msg_type_from_buf):
            case MessageType.Movie:
                msg_type = MessageType.Movie
                decoder = MovieProtocol.from_bytes

        data = decoder(buf[Message.MSG_TYPE_LEN + Message.MSG_LEN_SIZE:], bytes_amount)

        return Message(msg_type, data)

    @staticmethod
    def __int_from_bytes(buf: bytes, to: int) -> int:
        return int.from_bytes(buf[0:to], 'big')
