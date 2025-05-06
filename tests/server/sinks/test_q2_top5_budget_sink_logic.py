import unittest

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_budget_counter import MovieBudgetCounter
from src.server.sinks.q2_top5_budget_sink_logic import Q2Top5BudgetSinkLogic


class TestQ2Top5BudgetSinkLogic:
    def test_group_by_user_id_success(self):
        q2 = Q2Top5BudgetSinkLogic()

        movies_user_1 = [
            MovieBudgetCounter('Spain', 200),
            MovieBudgetCounter('Argentina', 100),
            MovieBudgetCounter('Brazil', 50),
        ]

        user_id_1 = 1

        movies_user_2 = [MovieBudgetCounter('Uruguay', 150)]

        user_id_2 = 2

        q2.merge_results(
            Message(user_id_1, MessageType.MovieBudgetCounter, movies_user_1)
        )
        q2.merge_results(
            Message(user_id_2, MessageType.MovieBudgetCounter, movies_user_2)
        )

        result_1 = q2.message_result(user_id_1)

        assert result_1.user_id == user_id_1

        data: list[MovieBudgetCounter] = result_1.data

        assert len(data) == len(movies_user_1)

        for i in range(len(data)):
            assert data[i] == movies_user_1[i]

        result_2 = q2.message_result(user_id_2)

        assert result_2.user_id == user_id_2

        data: list[MovieBudgetCounter] = result_2.data

        assert len(data) == len(movies_user_2)

        for i in range(len(data)):
            assert data[i] == movies_user_2[i]

    def test_group_by_user_id_with_no_movies_returns_empty_list(self):
        q2 = Q2Top5BudgetSinkLogic()

        user_id = 1

        result = q2.message_result(user_id)

        assert result.user_id == user_id
        assert len(result.data) == 0

    def test_message_result_for_same_user_id_returns_empty_after_first_call(self):
        q2 = Q2Top5BudgetSinkLogic()

        movies = [MovieBudgetCounter('Uruguay', 150)]

        user_id = 1

        q2.merge_results(Message(user_id, MessageType.MovieBudgetCounter, movies))
        result = q2.message_result(user_id)

        assert result.user_id == user_id
        assert len(result.data) == len(movies)
        for i in range(len(result.data)):
            assert result.data[i] == movies[i]

        result = q2.message_result(user_id)

        assert result.user_id == user_id
        assert len(result.data) == 0


if __name__ == '__main__':
    unittest.main()
