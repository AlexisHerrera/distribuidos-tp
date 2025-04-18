# pylint: disable=no-member
from src.messaging.protobuf import movie_budget_counters_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.movie_budget_counter import MovieBudgetCounter


class MovieBudgetCounterProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(item_to_bytes=self.__to_movie_budget_counter_pb,
                        encode_all=self.__encode_all,
                        bytes_to_item=self.__to_movie_budget_counter,
                        decode_all=self.__decode_all)

    def __to_movie_budget_counter_pb(self, movie_budget_counter: MovieBudgetCounter):
        movie_budget_counter_encoded = movie_budget_counters_pb2.MovieBudgetCounter()

        movie_budget_counter_encoded.country = movie_budget_counter.country
        movie_budget_counter_encoded.total_budget = movie_budget_counter.total_budget

        return movie_budget_counter_encoded

    def __encode_all(self, l):
        return movie_budget_counters_pb2.MovieBudgetCounters(list=l).SerializeToString()

    def __to_movie_budget_counter(self, movie_budget_counter_pb2) -> MovieBudgetCounter:
        country = movie_budget_counter_pb2.country
        total_budget = movie_budget_counter_pb2.total_budget

        return MovieBudgetCounter(country, total_budget)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_list = movie_budget_counters_pb2.MovieBudgetCounters()

        pb2_list.ParseFromString(buf[0:bytes_amount])

        return pb2_list.list
