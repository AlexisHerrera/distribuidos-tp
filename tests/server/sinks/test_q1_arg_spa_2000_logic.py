import unittest

from src.messaging.protocol.message import Message, MessageType
from src.model.movie import Movie
from src.server.sinks.q1_arg_spa_2000_sink_logic import Q1ArgSpa2000


class TestQ1ArgSpa2000SinkLogic:
    def test_group_by_user_id_success(self):
        q1 = Q1ArgSpa2000()

        movies_user_1 = [
            Movie(1, 'La Cienaga', ['Comedy', 'Drama']),
            Movie(2, 'Burnt Money', ['Crime']),
        ]

        user_id_1 = 1

        movies_user_2 = [
            Movie(3, 'The City of No Limits', ['Thriller', 'Drama']),
        ]

        user_id_2 = 2

        q1.merge_results(Message(user_id_1, MessageType.Movie, movies_user_1))
        q1.merge_results(Message(user_id_2, MessageType.Movie, movies_user_2))

        result_1 = q1.message_result(user_id_1)

        assert result_1.user_id == user_id_1

        data: list[Movie] = result_1.data

        assert len(data) == len(movies_user_1)

        for i in range(len(data)):
            assert data[i] == movies_user_1[i]

        result_2 = q1.message_result(user_id_2)

        assert result_2.user_id == user_id_2

        data: list[Movie] = result_2.data

        assert len(data) == len(movies_user_2)

        for i in range(len(data)):
            assert data[i] == movies_user_2[i]

    def test_group_by_user_id_with_no_movies_returns_empty_list(self):
        q1 = Q1ArgSpa2000()

        user_id = 1

        result = q1.message_result(user_id)

        assert result.user_id == user_id
        assert len(result.data) == 0


if __name__ == '__main__':
    unittest.main()
