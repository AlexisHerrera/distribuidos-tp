from src.messaging.protobuf import movie_avg_budgets_pb2
from src.messaging.protocol.movie_avg_budget import MovieAvgBudgetProtocol
from src.model.movie_avg_budget import MovieAvgBudget


class TestMovieAvgBudgetProtocol:
    def test_to_bytes(self):
        movie = MovieAvgBudget(4.0, 1.0)
        protocol = MovieAvgBudgetProtocol()

        res_encoded, res_bytes_amount = protocol.to_bytes(movie)

        movie_encoded = movie_avg_budgets_pb2.MovieAvgBudget()
        movie_encoded.positive = movie.positive
        movie_encoded.negative = movie.negative

        movies_encoded = movie_avg_budgets_pb2.MovieAvgBudget(
            positive=movie.positive, negative=movie.negative
        ).SerializeToString()
        bytes_amount = len(movies_encoded)

        assert res_encoded == movies_encoded
        assert res_bytes_amount == bytes_amount

    def test_from_bytes(self):
        movie = MovieAvgBudget(4.0, 1.0)
        protocol = MovieAvgBudgetProtocol()

        res_encoded, res_bytes_amount = protocol.to_bytes(movie)

        res = protocol.from_bytes(res_encoded, res_bytes_amount)

        assert movie.positive == res.positive
        assert movie.negative == res.negative
