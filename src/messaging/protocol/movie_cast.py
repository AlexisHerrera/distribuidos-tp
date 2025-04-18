# pylint: disable=no-member
from src.messaging.protobuf import movie_casts_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.movie_cast import MovieCast


class MovieCastProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(item_to_bytes=self.__to_movie_cast_pb,
                        encode_all=self.__encode_all,
                        bytes_to_item=self.__to_movie_cast,
                        decode_all=self.__decode_all)

    def __to_movie_cast_pb(self, movie_cast: MovieCast):
        movie_cast_encoded = movie_casts_pb2.MovieCast()

        movie_cast_encoded.id = movie_cast.movie_id
        movie_cast_encoded.title = movie_cast.title
        movie_cast_encoded.actors_name.extend(movie_cast.actors_name)

        return movie_cast_encoded

    def __encode_all(self, l):
        return movie_casts_pb2.MovieCasts(list=l).SerializeToString()

    def __to_movie_cast(self, movie_cast_pb2) -> MovieCast:
        movie_id = movie_cast_pb2.id
        title = movie_cast_pb2.title
        actors_name = movie_casts_pb2.actors_name

        return MovieCast(movie_id, title, actors_name)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_list = movie_casts_pb2.MovieCasts()

        pb2_list.ParseFromString(buf[0:bytes_amount])

        return pb2_list.list
