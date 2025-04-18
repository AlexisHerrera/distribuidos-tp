import unittest

from src.messaging.protocol.message import Message, MessageType
from src.model.cast import Cast
from src.model.movie import Movie
from src.model.rating import Rating

class TestMessage:
    class TestMovie:
        def test_encode_movie(self):
            movie = Movie(1, "Toy Story")
            message = Message(MessageType.Movie, [movie])

            message_bytes = message.to_bytes()

            assert message_bytes[0:1] == b'\x01'

            bytes_amount = int.from_bytes(message_bytes[1:3], 'big')

            assert len(message_bytes[3:]) == bytes_amount

        def test_encode_and_decode_movie_should_return_same_movie(self):
            movie = Movie(1, "Toy Story")
            message = Message(MessageType.Movie, [movie])

            message_bytes = message.to_bytes()

            result = Message.from_bytes(message_bytes)

            assert result.message_type == MessageType.Movie
            assert result.data is not None
            assert len(result.data) == 1

            result_movie = result.data[0]

            assert result_movie.id == movie.id
            assert result_movie.title == movie.title


    class TestRating:
        def test_encode_rating(self):
            rating = Rating(1, 4.5)
            message = Message(MessageType.Rating, [rating])

            message_bytes = message.to_bytes()

            assert message_bytes[0:1] == b'\x02'

            bytes_amount = int.from_bytes(message_bytes[1:3], 'big')

            assert len(message_bytes[3:]) == bytes_amount

        def test_encode_and_decode_rating_should_return_same_rating(self):
            rating = Rating(1, 4.5)
            message = Message(MessageType.Rating, [rating])

            message_bytes = message.to_bytes()

            result = Message.from_bytes(message_bytes)

            assert result.message_type == MessageType.Rating
            assert result.data is not None
            assert len(result.data) == 1

            result_rating = result.data[0]

            assert result_rating.movie_id == rating.movie_id
            assert result_rating.rating == rating.rating

    class TestCast:
        def test_encode_cast(self):
            cast = Cast(1, ["Ricardo Darín", "Guillermo Francella"])
            message = Message(MessageType.Cast, [cast])

            message_bytes = message.to_bytes()

            assert message_bytes[0:1] == b'\x03'

            bytes_amount = int.from_bytes(message_bytes[1:3], 'big')

            assert len(message_bytes[3:]) == bytes_amount

        def test_encode_and_decode_cast_should_return_same_cast(self):
            cast = Cast(1, ["Ricardo Darín", "Guillermo Francella"])
            message = Message(MessageType.Cast, [cast])

            message_bytes = message.to_bytes()

            result = Message.from_bytes(message_bytes)

            assert result.message_type == MessageType.Cast
            assert result.data is not None
            assert len(result.data) == 1

            result_cast = result.data[0]

            assert result_cast.id == cast.id
            for i, name in enumerate(cast.cast):
                assert result_cast.cast[i] == name

    class TestEOF:
        def test_encode_eof_should_return_empty_data(self):
            message = Message(MessageType.EOF, None)

            result = message.to_bytes()

            assert result[:Message.MSG_TYPE_LEN] == MessageType.EOF.value.to_bytes()
            assert result[Message.MSG_TYPE_LEN:Message.MSG_TYPE_LEN + Message.MSG_LEN_SIZE] == b'\x00\x00'

        def test_decode_eof_should_return_eof_type(self):
            message = Message(MessageType.EOF, None)

            message_bytes = message.to_bytes()

            result = Message.from_bytes(message_bytes)

            assert result.message_type == message.message_type
            assert result.data == message.data



if __name__ == "__main__":
    unittest.main()
