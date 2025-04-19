from src.messaging.protobuf import casts_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.cast import Cast


class CastProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(
            item_to_bytes=self.__to_cast_pb,
            encode_all=self.__encode_all,
            bytes_to_item=self.__to_cast,
            decode_all=self.__decode_all,
        )

    def __to_cast_pb(self, cast: Cast):
        cast_encoded = casts_pb2.Cast()

        cast_encoded.id = cast.id
        cast_encoded.cast.extend(cast.cast)

        return cast_encoded

    def __encode_all(self, a_list):
        return casts_pb2.Casts(list=a_list).SerializeToString()

    def __to_cast(self, cast_pb2) -> Cast:
        movie_id = cast_pb2.id
        cast = cast_pb2.cast

        return Cast(movie_id, cast)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_list = casts_pb2.Casts()

        pb2_list.ParseFromString(buf[0:bytes_amount])

        return pb2_list.list
