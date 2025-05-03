import unittest

from src.messaging.protocol.message import Message, MessageType
from src.model.cast import Cast
from src.model.movie import Movie
from src.model.rating import Rating


class TestMessage:
    @staticmethod
    def int_to_bytes(value: int, length: int = 1) -> bytes:
        return value.to_bytes(length=length, byteorder='big', signed=False)

    @staticmethod
    def int_from_bytes(buf: bytes, bytes_amount: int) -> int:
        return int.from_bytes(buf[0:bytes_amount], 'big', signed=False)

    def test_message_user_id(self):
        user_id = 1
        message = Message(user_id, MessageType.EOF)
        message_bytes = message.to_bytes()

        assert message_bytes[
            Message.USER_ID_POS : Message.USER_ID_SIZE
        ] == self.int_to_bytes(user_id, Message.USER_ID_SIZE)

    class TestMovie:
        def test_encode_movie(self):
            movie = Movie(1, 'Toy Story')
            user_id = 1
            message = Message(user_id, MessageType.Movie, [movie])

            message_bytes = message.to_bytes()

            assert message_bytes[
                Message.USER_ID_POS : Message.USER_ID_SIZE
            ] == TestMessage.int_to_bytes(user_id, message.USER_ID_SIZE)

            assert message_bytes[
                Message.MSG_TYPE_POS : Message.MSG_TYPE_POS + Message.MSG_TYPE_SIZE
            ] == TestMessage.int_to_bytes(MessageType.Movie.value)

            bytes_amount = TestMessage.int_from_bytes(
                message_bytes[Message.MSG_LEN_POS :],
                Message.MSG_LEN_SIZE,
            )

            assert len(message_bytes[Message.MSG_DATA_POS :]) == bytes_amount

        def test_encode_and_decode_movie_should_return_same_movie(self):
            movie = Movie(1, 'Toy Story')
            user_id = 1
            message = Message(user_id, MessageType.Movie, [movie])

            message_bytes = message.to_bytes()

            result = Message.from_bytes(message_bytes)

            assert result.user_id == user_id
            assert result.message_type == MessageType.Movie
            assert result.data is not None
            assert len(result.data) == 1

            result_movie = result.data[0]

            assert result_movie.id == movie.id
            assert result_movie.title == movie.title

    class TestRating:
        def test_encode_rating(self):
            rating = Rating(1, 4.5)
            user_id = 1
            message = Message(user_id, MessageType.Rating, [rating])

            message_bytes = message.to_bytes()

            assert message_bytes[
                Message.USER_ID_POS : Message.USER_ID_SIZE
            ] == TestMessage.int_to_bytes(user_id, Message.USER_ID_SIZE)

            assert message_bytes[
                Message.MSG_TYPE_POS : Message.MSG_TYPE_POS + Message.MSG_TYPE_SIZE
            ] == TestMessage.int_to_bytes(MessageType.Rating.value)

            bytes_amount = TestMessage.int_from_bytes(
                message_bytes[Message.MSG_LEN_POS :], Message.MSG_LEN_SIZE
            )

            assert len(message_bytes[Message.MSG_DATA_POS :]) == bytes_amount

        def test_encode_and_decode_rating_should_return_same_rating(self):
            rating = Rating(1, 4.5)
            user_id = 1
            message = Message(user_id, MessageType.Rating, [rating])

            message_bytes = message.to_bytes()

            result = Message.from_bytes(message_bytes)

            assert result.user_id == user_id
            assert result.message_type == MessageType.Rating
            assert result.data is not None
            assert len(result.data) == 1

            result_rating = result.data[0]

            assert result_rating.movie_id == rating.movie_id
            assert result_rating.rating == rating.rating

    class TestCast:
        def test_encode_cast(self):
            cast = Cast(1, ['Ricardo Darín', 'Guillermo Francella'])
            user_id = 1
            message = Message(user_id, MessageType.Cast, [cast])

            message_bytes = message.to_bytes()

            assert message_bytes[
                Message.MSG_TYPE_POS : Message.MSG_TYPE_POS + Message.MSG_TYPE_SIZE
            ] == TestMessage.int_to_bytes(MessageType.Cast.value)

            bytes_amount = TestMessage.int_from_bytes(
                message_bytes[Message.MSG_LEN_POS :], Message.MSG_LEN_SIZE
            )

            assert len(message_bytes[Message.MSG_DATA_POS :]) == bytes_amount

        def test_encode_and_decode_cast_should_return_same_cast(self):
            cast = Cast(1, ['Ricardo Darín', 'Guillermo Francella'])
            user_id = 1
            message = Message(user_id, MessageType.Cast, [cast])

            message_bytes = message.to_bytes()

            result = Message.from_bytes(message_bytes)

            assert result.user_id == user_id
            assert result.message_type == MessageType.Cast
            assert result.data is not None
            assert len(result.data) == 1

            result_cast = result.data[0]

            assert result_cast.id == cast.id
            for i, name in enumerate(cast.cast):
                assert result_cast.cast[i] == name

    class TestEOF:
        def test_encode_eof_should_return_empty_data(self):
            user_id = 1
            message = Message(user_id, MessageType.EOF)

            result = message.to_bytes()

            assert result[
                Message.MSG_TYPE_POS : Message.MSG_TYPE_POS + Message.MSG_TYPE_SIZE
            ] == TestMessage.int_to_bytes(MessageType.EOF.value, Message.MSG_TYPE_SIZE)
            assert result[
                Message.MSG_LEN_POS : Message.MSG_LEN_POS + Message.MSG_LEN_SIZE
            ] == TestMessage.int_to_bytes(0, Message.MSG_LEN_SIZE)

        def test_decode_eof_should_return_eof_type(self):
            user_id = 1
            message = Message(user_id, MessageType.EOF, None)

            message_bytes = message.to_bytes()

            result = Message.from_bytes(message_bytes)

            assert result.user_id == user_id
            assert result.message_type == message.message_type
            assert result.data == message.data


if __name__ == '__main__':
    unittest.main()
