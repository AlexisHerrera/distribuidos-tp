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

    def __to_movie_avg_budget_pb2(self, movie_avg_budget: MovieAvgBudget):
        movie_avg_budget_encoded = movie_avg_budgets_pb2.MovieAvgBudget()

        movie_avg_budget_encoded.id = movie_avg_budget.movie_id
        movie_avg_budget_encoded.title = movie_avg_budget.title
        movie_avg_budget_encoded.avg_budget_revenue = (
            movie_avg_budget.avg_budget_revenue
        )
        movie_avg_budget_encoded.sentiment = movie_avg_budget.sentiment

        return movie_avg_budget_encoded

    def __encode_all(self, a_list):
        return movie_avg_budgets_pb2.MovieAvgBudgets(list=a_list).SerializeToString()

    def __to_movie_avg_budget(self, movie_avg_budget_pb2) -> MovieAvgBudget:
        movie_id = movie_avg_budget_pb2.id
        title = movie_avg_budget_pb2.title
        avg_budget_revenue = movie_avg_budget_pb2.avg_budget_revenue
        sentiment = movie_avg_budget_pb2.sentiment

        return MovieAvgBudget(movie_id, title, avg_budget_revenue, sentiment)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        pb2_list = movie_avg_budgets_pb2.MovieAvgBudgets()

        pb2_list.ParseFromString(buf[0:bytes_amount])

        return pb2_list.list
