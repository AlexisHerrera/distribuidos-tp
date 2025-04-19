from enum import Enum

from src.messaging.protocol.actor_count import ActorCountProtocol
from src.messaging.protocol.cast import CastProtocol
from src.messaging.protocol.eof import EOFProtocol
from src.messaging.protocol.message_protocol import MessageProtocol
from src.messaging.protocol.movie import MovieProtocol
from src.messaging.protocol.movie_avg_budget import MovieAvgBudgetProtocol
from src.messaging.protocol.movie_budget_counter import MovieBudgetCounterProtocol
from src.messaging.protocol.movie_cast import MovieCastProtocol
from src.messaging.protocol.movie_rating import MovieRatingProtocol
from src.messaging.protocol.movie_rating_avg import MovieRatingAvgProtocol
from src.messaging.protocol.movie_sentiment import MovieSentimentProtocol
from src.messaging.protocol.null import NullProtocol
from src.messaging.protocol.rating import RatingProtocol


class MessageType(Enum):
    Unknown = 0
    Movie = 1
    Rating = 2
    Cast = 3
    MovieSentiment = 4
    MovieAvgBudget = 5
    MovieBudgetCounter = 6
    MovieRating = 7
    MovieRatingAvg = 8
    MovieCast = 9
    ActorCount = 10
    EOF = 100

    @classmethod
    def _missing_(cls, _value):
        return MessageType.Unknown


class Message:
    MSG_TYPE_LEN = 1
    MSG_LEN_SIZE = 2

    def __init__(self, message_type: MessageType, data: object):
        self.message_type = message_type
        self.data = data

    def to_bytes(self) -> bytes:
        encoder = Message.__get_protocol(self.message_type)

        (data_encoded, bytes_amount) = encoder.to_bytes(self.data)
        msg_type_encoded = Message.__int_to_bytes(
            self.message_type.value, Message.MSG_TYPE_LEN
        )
        bytes_amount_encoded = Message.__int_to_bytes(
            bytes_amount, Message.MSG_LEN_SIZE
        )

        return msg_type_encoded + bytes_amount_encoded + data_encoded

    @staticmethod
    def from_bytes(buf: bytes):
        msg_type_from_buf = Message.__int_from_bytes(buf, Message.MSG_TYPE_LEN)
        bytes_amount = Message.__int_from_bytes(
            buf[Message.MSG_TYPE_LEN :], Message.MSG_LEN_SIZE
        )

        msg_type = MessageType(msg_type_from_buf)
        decoder = Message.__get_protocol(msg_type)

        data: list | None = decoder.from_bytes(
            buf[Message.MSG_TYPE_LEN + Message.MSG_LEN_SIZE :], bytes_amount
        )

        return Message(msg_type, data)

    @staticmethod
    def __int_from_bytes(buf: bytes, bytes_amount: int) -> int:
        return int.from_bytes(buf[0:bytes_amount], 'big', signed=False)

    @staticmethod
    def __int_to_bytes(value: int, bytes_amount: int) -> bytes:
        return value.to_bytes(bytes_amount, 'big', signed=False)

    @staticmethod
    def __get_protocol(msg_type: MessageType) -> MessageProtocol:
        match msg_type:
            case MessageType.Movie:
                return MovieProtocol()
            case MessageType.Rating:
                return RatingProtocol()
            case MessageType.Cast:
                return CastProtocol()
            case MessageType.MovieSentiment:
                return MovieSentimentProtocol()
            case MessageType.MovieAvgBudget:
                return MovieAvgBudgetProtocol()
            case MessageType.MovieBudgetCounter:
                return MovieBudgetCounterProtocol()
            case MessageType.MovieRating:
                return MovieRatingProtocol()
            case MessageType.MovieRatingAvg:
                return MovieRatingAvgProtocol()
            case MessageType.MovieCast:
                return MovieCastProtocol()
            case MessageType.ActorCount:
                return ActorCountProtocol()
            case MessageType.EOF:
                return EOFProtocol()
            case _:
                return NullProtocol()
