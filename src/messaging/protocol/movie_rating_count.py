from src.messaging.protobuf import movie_ratings_count_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.movie_rating_count import MovieRatingCount


class MovieRatingCounterProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(
            item_to_bytes=self.__to_movie_rating_count_pb,
            encode_all=self.__encode_all,
            bytes_to_item=self.__to_movie_rating_avg,
            decode_all=self.__decode_all,
        )

    def __to_movie_rating_count_pb(self, movie_rating_count: MovieRatingCount):
        movie_rating_count_encoded = movie_ratings_count_pb2.MovieRatingAvg()

        movie_rating_count_encoded.id = movie_rating_count.movie_id
        movie_rating_count_encoded.title = movie_rating_count.title
        movie_rating_count_encoded.partial_sum = movie_rating_count.partial_sum
        movie_rating_count_encoded.count = movie_rating_count.count

        return movie_rating_count_encoded

    def __encode_all(self, a_list):
        return movie_ratings_count_pb2.MovieRatingAvgs(list=a_list).SerializeToString()

    def __to_movie_rating_avg(self, movie_rating_count_pb2) -> MovieRatingCount:
        movie_id = movie_rating_count_pb2.id
        title = movie_rating_count_pb2.title
        partial_sum = movie_rating_count_pb2.partial_sum
        count = movie_rating_count_pb2.count

        return MovieRatingCount(movie_id, title, partial_sum, count)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_list = movie_ratings_count_pb2.MovieRatingAvgs()

        pb2_list.ParseFromString(buf[0:bytes_amount])

        return pb2_list.list
