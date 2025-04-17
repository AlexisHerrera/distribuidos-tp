# pylint: disable=no-member
from src.messaging.protobuf import ratings_pb2
from src.model.rating import Rating


class RatingProtocol:
    @staticmethod
    def to_bytes(ratings: list[Rating]) -> tuple[bytes, int]:
        ratings_pb2_list = []

        for rating in ratings:
            ratings_pb2_list.append(RatingProtocol.to_rating_pb(rating))

        ratings_encoded = ratings_pb2.Ratings(list=ratings_pb2_list).SerializeToString()

        return ratings_encoded, len(ratings_encoded)

    @staticmethod
    def to_rating_pb(rating: Rating):
        rating_encoded = ratings_pb2.Rating()

        rating_encoded.movie_id = rating.movie_id
        rating_encoded.rating = rating.rating

        return rating_encoded

    @staticmethod
    def from_bytes(buf: bytes, bytes_amount: int) -> list[Rating]:
        ratings_pb2_list = ratings_pb2.Ratings()

        ratings_pb2_list.ParseFromString(buf[0:bytes_amount])

        ratings = []

        for rating_pb2 in ratings_pb2_list.list:
            ratings.append(RatingProtocol.to_rating(rating_pb2))

        return ratings

    @staticmethod
    def to_rating(rating_pb2) -> Rating:
        movie_id = rating_pb2.movie_id
        rating = rating_pb2.rating

        return Rating(movie_id, rating)
