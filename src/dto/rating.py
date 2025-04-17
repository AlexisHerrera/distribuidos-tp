# pylint: disable=no-member
from src.messaging.list_decoder import ListDecoder
from src.messaging.list_encoder import ListEncoder
from src.messaging.protobuf import ratings_pb2
from src.model.rating import Rating


class RatingProtocol:
    def __init__(self):
        self.__encoder = ListEncoder(item_to_bytes=self.__to_rating_pb,
                        encode_all=lambda l: ratings_pb2.Ratings(list=l).SerializeToString())

        self.__decoder = ListDecoder(bytes_to_item=self.__to_rating, decode_all=self.__decode_all)

    def to_bytes(self, ratings: list[Rating]) -> tuple[bytes, int]:
        return self.__encoder.to_bytes(ratings)

    def __to_rating_pb(self, rating: Rating):
        rating_encoded = ratings_pb2.Rating()

        rating_encoded.movie_id = rating.movie_id
        rating_encoded.rating = rating.rating

        return rating_encoded

    def from_bytes(self, buf: bytes, bytes_amount: int) -> list[Rating]:
        return self.__decoder.from_bytes(buf, bytes_amount)

    def __to_rating(self, rating_pb2) -> Rating:
        movie_id = rating_pb2.movie_id
        rating = rating_pb2.rating

        return Rating(movie_id, rating)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        ratings_pb2_list = ratings_pb2.Ratings()

        ratings_pb2_list.ParseFromString(buf[0:bytes_amount])

        return ratings_pb2_list.list
