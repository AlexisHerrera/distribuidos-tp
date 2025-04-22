import unittest

from src.model.movie import Movie
from src.server.filters.argentina_filter import ArgentinaLogic


class TestArgentinaFilter:
    def test_filter_argentina_movie_with_argentina_returns_true(self):
        movie = Movie(
            movie_id=1,
            title='Random movie',
            production_countries=['Argentina', 'Spain'],
        )

        logic = ArgentinaLogic()

        assert logic.should_pass(movie)

    def test_filter_argentina_movie_without_argentina_returns_false(self):
        movie = Movie(
            movie_id=1,
            title='Random movie',
            production_countries=['Brazil'],
        )

        logic = ArgentinaLogic()

        assert not logic.should_pass(movie)


if __name__ == '__main__':
    unittest.main()
