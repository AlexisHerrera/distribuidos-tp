from src.messaging.protobuf import actor_counts_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.actor_count import ActorCount


class ActorCountProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(
            item_to_bytes=self.__to_actor_count_pb2,
            encode_all=self.__encode_all,
            bytes_to_item=self.__to_actor_count,
            decode_all=self.__decode_all,
        )

    def __to_actor_count_pb2(self, actor_count: ActorCount):
        actor_count_encoded = actor_counts_pb2.ActorCount()

        actor_count_encoded.actor_name = actor_count.actor_name
        actor_count_encoded.count = actor_count.count

        return actor_count_encoded

    def __encode_all(self, a_list):
        return actor_counts_pb2.ActorCounts(list=a_list).SerializeToString()

    def __to_actor_count(self, actor_count_pb2) -> ActorCount:
        actor_name = actor_count_pb2.actor_name
        count = actor_count_pb2.count

        return ActorCount(actor_name, count)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_list = actor_counts_pb2.ActorCounts()

        pb2_list.ParseFromString(buf[0:bytes_amount])

        return pb2_list.list
