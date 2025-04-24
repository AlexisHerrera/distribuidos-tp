from src.messaging.protobuf import movie_rating_avgs_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.movie_rating_avg import MovieRatingAvg


class MovieRatingAvgProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(
            item_to_bytes=self.__to_movie_rating_avg_pb,
            encode_all=self.__encode_all,
            bytes_to_item=self.__to_movie_rating_avg,
            decode_all=self.__decode_all,
        )

    def __to_movie_rating_avg_pb(self, movie_rating_avg: MovieRatingAvg):
        movie_rating_avg_encoded = movie_rating_avgs_pb2.MovieRatingAvg()

        movie_rating_avg_encoded.id = movie_rating_avg.id
        movie_rating_avg_encoded.title = movie_rating_avg.title
        movie_rating_avg_encoded.average_rating = movie_rating_avg.average_rating

        return movie_rating_avg_encoded

    def __encode_all(self, a_dict):
        return movie_rating_avgs_pb2.MovieRatingAvgs(
            min=a_dict['min'], max=a_dict['max']
        ).SerializeToString()

    def to_bytes(self, items: dict):
        items_pb2 = {}

        for k, v in items.items():
            items_pb2[k] = self.__to_movie_rating_avg_pb(v)

        items_encoded = self.__encode_all(items_pb2)

        return items_encoded, len(items_encoded)

    def __to_movie_rating_avg(self, movie_rating_avg_pb2) -> MovieRatingAvg:
        movie_id = movie_rating_avg_pb2.id
        title = movie_rating_avg_pb2.title
        average_rating = movie_rating_avg_pb2.average_rating

        return MovieRatingAvg(movie_id, title, average_rating)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_dict = movie_rating_avgs_pb2.MovieRatingAvgs()

        pb2_dict.ParseFromString(buf[0:bytes_amount])

        return {'min': pb2_dict.min, 'max': pb2_dict.max}

    def from_bytes(self, buf: bytes, bytes_amount: int) -> dict:
        items = self.__decode_all(buf, bytes_amount)

        result = {}

        for k, v in items.items():
            result[k] = self.__to_movie_rating_avg(v)

        return result
