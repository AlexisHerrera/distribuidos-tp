import unittest

from src.messaging.message import Message, MessageType
from src.model.movie import Movie

class TestMessage:
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
