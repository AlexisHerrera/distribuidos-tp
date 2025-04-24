from src.messaging.protobuf import movie_avg_budgets_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.movie_avg_budget import MovieAvgBudget


class MovieAvgBudgetProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(
            item_to_bytes=self.__to_movie_avg_budget_pb2,
            encode_all=self.__encode_all,
            bytes_to_item=self.__to_movie_avg_budget,
            decode_all=self.__decode_all,
        )

    def to_bytes(self, item):
        encoded = self.__to_movie_avg_budget_pb2(item).SerializeToString()

        return encoded, len(encoded)

    def __encode_all(self, obj):
        pass

    def __to_movie_avg_budget_pb2(self, movie_avg_budget: MovieAvgBudget):
        movie_avg_budget_encoded = movie_avg_budgets_pb2.MovieAvgBudget()

        movie_avg_budget_encoded.positive = movie_avg_budget.positive
        movie_avg_budget_encoded.negative = movie_avg_budget.negative

        return movie_avg_budget_encoded

    def from_bytes(self, buf: bytes, bytes_amount: int):
        obj = self.__decode_all(buf, bytes_amount)

        return self.__to_movie_avg_budget(obj)

    def __to_movie_avg_budget(self, movie_avg_budget_pb2) -> MovieAvgBudget:
        positive = movie_avg_budget_pb2.positive
        negative = movie_avg_budget_pb2.negative

        return MovieAvgBudget(positive, negative)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_obj = movie_avg_budgets_pb2.MovieAvgBudget()

        pb2_obj.ParseFromString(buf[0:bytes_amount])

        return pb2_obj
