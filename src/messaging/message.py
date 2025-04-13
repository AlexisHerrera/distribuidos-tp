from enum import Enum

from src.dto.movie import MovieProtocol


class MessageType(Enum):
    Movie = 1


class Message():
    def __init__(self, message_type: MessageType, data: object):
        self.message_type = message_type
        self.data = data

    def to_bytes(self) -> bytes:
        match self.message_type:
            case MessageType.Movie:
                return MovieProtocol.to_bytes(self.data)

        return b''

    @staticmethod
    def from_bytes(buf: bytes):
        pass
