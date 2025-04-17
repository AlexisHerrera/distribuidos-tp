# pylint: disable=no-member
import unittest

from src.dto.movie import MovieProtocol
from src.messaging.protobuf import movies_pb2
from src.model.movie import Movie


class TestMovieProtocol:
    def test_to_bytes_empty_list(self):
        protocol = MovieProtocol()
        res_encoded, res_bytes_amount = protocol.to_bytes([])

        expected_encoded, expected_bytes_amount = b'', 0

        assert res_encoded == expected_encoded
        assert res_bytes_amount == expected_bytes_amount

    def test_to_bytes(self):
        movie = Movie(movie_id=1, title="Toy Story")
        protocol = MovieProtocol()

        res_encoded, res_bytes_amount = protocol.to_bytes([movie])

        movie_encoded = movies_pb2.Movie()
        movie_encoded.id = movie.id
        movie_encoded.title = movie.title

        movies_encoded = movies_pb2.Movies(list=[movie_encoded]).SerializeToString()
        bytes_amount = len(movies_encoded)

        assert res_encoded == movies_encoded
        assert res_bytes_amount == bytes_amount

    def test_from_bytes(self):
        movie = Movie(movie_id=1, title="Toy Story")
        movie_encoded = movies_pb2.Movie()
        movie_encoded.id = movie.id
        movie_encoded.title = movie.title

        movies_encoded = movies_pb2.Movies(list=[movie_encoded]).SerializeToString()
        bytes_amount = len(movies_encoded)

        protocol = MovieProtocol()

        result = protocol.from_bytes(movies_encoded, bytes_amount)

        assert len(result) == 1
        assert result[0].id == movie.id
        assert result[0].title == movie.title


if __name__ == "__main__":
    unittest.main()
