from src.messaging.protobuf import movie_ratings_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.movie_rating import MovieRating


class MovieRatingProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(
            item_to_bytes=self.__to_movie_rating_pb,
            encode_all=self.__encode_all,
            bytes_to_item=self.__to_movie_rating,
            decode_all=self.__decode_all,
        )

    def __to_movie_rating_pb(self, movie_rating: MovieRating):
        movie_rating_encoded = movie_ratings_pb2.MovieRating()

        movie_rating_encoded.id = movie_rating.movie_id
        movie_rating_encoded.title = movie_rating.title
        movie_rating_encoded.rating = movie_rating.rating

        return movie_rating_encoded

    def __encode_all(self, a_list):
        return movie_ratings_pb2.MovieRatings(list=a_list).SerialiazeToString()

    def __to_movie_rating(self, movie_rating_pb2) -> MovieRating:
        movie_id = movie_rating_pb2.id
        title = movie_rating_pb2.title
        rating = movie_rating_pb2.rating

        return MovieRating(movie_id, title, rating)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_list = movie_ratings_pb2.MovieRatings()

        pb2_list.ParseFromString(buf[0:bytes_amount])

        return pb2_list.list
