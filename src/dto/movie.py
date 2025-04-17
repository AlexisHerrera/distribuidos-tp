# pylint: disable=no-member
from src.messaging.protobuf import movies_pb2
from src.model.movie import Movie


class MovieProtocol:
    @staticmethod
    def to_bytes(movies: list[Movie]) -> tuple[bytes, int]:
        movies_pb2_list = []

        for movie in movies:
            movies_pb2_list.append(MovieProtocol.to_movie_pb(movie))

        movies_encoded = movies_pb2.Movies(list=movies_pb2_list).SerializeToString()

        return movies_encoded, len(movies_encoded)

    @staticmethod
    def to_movie_pb(movie: Movie):
        movie_encoded = movies_pb2.Movie()

        movie_encoded.id = movie.id
        movie_encoded.title = movie.title

        return movie_encoded

    @staticmethod
    def from_bytes(buf: bytes, bytes_amount: int) -> list[Movie]:
        movies_pb2_list = movies_pb2.Movies()

        movies_pb2_list.ParseFromString(buf[0:bytes_amount])

        movies = []

        for movie_pb2 in movies_pb2_list.list:
            movies.append(MovieProtocol.to_movie(movie_pb2))

        return movies

    @staticmethod
    def to_movie(movie_pb2) -> Movie:
        movie_id = movie_pb2.id
        title = movie_pb2.title

        return Movie(movie_id, title)
