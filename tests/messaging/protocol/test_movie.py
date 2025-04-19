import unittest

from src.messaging.protocol.movie import MovieProtocol
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
        movie = Movie(movie_id=1, title='Toy Story')
        protocol = MovieProtocol()

        res_encoded, res_bytes_amount = protocol.to_bytes([movie])

        movie_encoded = movies_pb2.Movie()
        movie_encoded.id = movie.id
        movie_encoded.title = movie.title
        movie_encoded.release_date = movie.release_date
        movie_encoded.budget = movie.budget
        movie_encoded.revenue = movie.revenue
        movie_encoded.overview = movie.overview

        movies_encoded = movies_pb2.Movies(list=[movie_encoded]).SerializeToString()
        bytes_amount = len(movies_encoded)

        assert res_encoded == movies_encoded
        assert res_bytes_amount == bytes_amount

    def test_from_bytes(self):
        movie = Movie(movie_id=1, title='Toy Story')
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

    def test_to_bytes_with_all_fields(self):
        movie = Movie(
            movie_id=1,
            title='Toy Story',
            genres=['Comedy', 'Family', 'Animation'],
            release_date='1995-10-30',
            production_countries=['United States of America'],
            budget=30000000,
            revenue=373554033,
            overview="Led by Woody, Andy's toys live happily in his room until Andy's\
                    birthday brings Buzz Lightyear onto ...",
        )

        protocol = MovieProtocol()

        res_encoded, res_bytes_amount = protocol.to_bytes([movie])

        movie_encoded = movies_pb2.Movie()
        movie_encoded.id = movie.id
        movie_encoded.title = movie.title
        movie_encoded.genres.extend(movie.genres)
        movie_encoded.release_date = movie.release_date
        movie_encoded.production_countries.extend(movie.production_countries)
        movie_encoded.budget = movie.budget
        movie_encoded.revenue = movie.revenue
        movie_encoded.overview = movie.overview

        movies_encoded = movies_pb2.Movies(list=[movie_encoded]).SerializeToString()
        bytes_amount = len(movies_encoded)

        assert res_encoded == movies_encoded
        assert res_bytes_amount == bytes_amount


if __name__ == '__main__':
    unittest.main()
