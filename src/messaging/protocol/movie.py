# pylint: disable=no-member
from src.messaging.protobuf import movies_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.movie import Movie


class MovieProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(item_to_bytes=self.__to_movie_pb,
                        encode_all=self.__encode_all,
                        bytes_to_item=self.__to_movie,
                        decode_all=self.__decode_all)

    def __to_movie_pb(self, movie: Movie):
        movie_encoded = movies_pb2.Movie()

        movie_encoded.id = movie.id
        movie_encoded.title = movie.title
        if movie.genres is not None:
            for genre in movie.genres:
                movie_encoded.genres.add(genre)

        movie_encoded.release_date = movie.release_date

        if movie.production_countries is not None:
            for country in movie.production_countries:
                movie_encoded.production_countries.add(country)

        movie_encoded.budget = movie.budget
        movie_encoded.revenue = movie.revenue
        movie_encoded.overview = movie.overview

        return movie_encoded

    def __encode_all(self, l):
        return movies_pb2.Movies(list=l).SerializeToString()

    def __to_movie(self, movie_pb2) -> Movie:
        movie_id = movie_pb2.id
        title = movie_pb2.title
        genres = movie_pb2.genres
        release_date = movie_pb2.release_date
        production_countries = movie_pb2.production_countries
        budget = movie_pb2.budget
        revenue = movie_pb2.revenue
        overview = movie_pb2.overview

        return Movie(movie_id, title, genres, release_date, production_countries, budget, revenue, overview)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        movies_pb2_list = movies_pb2.Movies()

        movies_pb2_list.ParseFromString(buf[0:bytes_amount])

        return movies_pb2_list.list
