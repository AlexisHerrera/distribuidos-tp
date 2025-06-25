import uuid
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
from src.messaging.protocol.movie_rating_count import MovieRatingCounterProtocol
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
    MovieRatingCounter = 8
    MovieCast = 9
    ActorCount = 10
    MovieRatingAvg = 11
    EOF = 100
    ACK = 101
    HANDSHAKE_SESSION = 102

    @classmethod
    def _missing_(cls, value):
        for member in cls:
            if member.name == value:
                return member

        return MessageType.Unknown


class Message:
    USER_ID_SIZE = 16
    MSG_ID_SIZE = 16
    MSG_TYPE_SIZE = 1
    MSG_LEN_SIZE = 2

    USER_ID_POS = 0
    MSG_ID_POS = USER_ID_POS + USER_ID_SIZE
    MSG_TYPE_POS = MSG_ID_POS + MSG_ID_SIZE
    MSG_LEN_POS = MSG_TYPE_POS + MSG_TYPE_SIZE
    MSG_DATA_POS = MSG_LEN_POS + MSG_LEN_SIZE

    def __init__(
        self,
        user_id: uuid.UUID,
        message_type: MessageType,
        data: object = None,
        message_id: uuid.UUID | None = None,
    ):
        self.user_id = user_id
        self.message_id = message_id or uuid.uuid4()
        self.message_type = message_type
        self.data = data

    def to_bytes(self) -> bytes:
        encoder = Message.__get_protocol(self.message_type)

        (data_encoded, bytes_amount) = encoder.to_bytes(self.data)

        user_id_encoded = self.user_id.bytes

        msg_id_encoded = self.message_id.bytes

        msg_type_encoded = Message.__int_to_bytes(
            self.message_type.value, Message.MSG_TYPE_SIZE
        )

        bytes_amount_encoded = Message.__int_to_bytes(
            bytes_amount, Message.MSG_LEN_SIZE
        )

        return (
            user_id_encoded
            + msg_id_encoded
            + msg_type_encoded
            + bytes_amount_encoded
            + data_encoded
        )

    @staticmethod
    def from_bytes(buf: bytes):
        user_id = uuid.UUID(
            bytes=buf[Message.USER_ID_POS : Message.USER_ID_POS + Message.USER_ID_SIZE]
        )

        message_id = uuid.UUID(
            bytes=buf[Message.MSG_ID_POS : Message.MSG_ID_POS + Message.MSG_ID_SIZE]
        )

        msg_type_from_buf = Message.__int_from_bytes(
            buf[Message.MSG_TYPE_POS :], Message.MSG_TYPE_SIZE
        )
        bytes_amount = Message.__int_from_bytes(
            buf[Message.MSG_LEN_POS :], Message.MSG_LEN_SIZE
        )

        msg_type = MessageType(msg_type_from_buf)
        decoder = Message.__get_protocol(msg_type)

        data: list | None = decoder.from_bytes(
            buf[Message.MSG_DATA_POS :], bytes_amount
        )

        return Message(user_id, msg_type, data, message_id)

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
            case MessageType.MovieRatingCounter:
                return MovieRatingCounterProtocol()
            case MessageType.MovieCast:
                return MovieCastProtocol()
            case MessageType.ActorCount:
                return ActorCountProtocol()
            case MessageType.MovieRatingAvg:
                return MovieRatingAvgProtocol()
            case MessageType.EOF:
                return EOFProtocol()
            case _:
                return NullProtocol()
