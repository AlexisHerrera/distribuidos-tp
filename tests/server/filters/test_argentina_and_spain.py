import unittest

from src.model.movie import Movie
from src.server.filters.argentina_spain_filter import ArgentinaAndSpainLogic


class TestArgentinaAndSpainFilter:
    def test_filter_argentina_and_spain_movie_with_both_countries_returns_true(self):
        movie = Movie(
            movie_id=1,
            title='Random movie',
            production_countries=['Argentina', 'Spain'],
        )

        logic = ArgentinaAndSpainLogic()

        assert logic.should_pass(movie)

    def test_filter_argentina_and_spain_movie_without_any_country_returns_false(self):
        movie = Movie(
            movie_id=1,
            title='Random movie',
            production_countries=['United States of America'],
        )

        logic = ArgentinaAndSpainLogic()

        assert not logic.should_pass(movie)

    def test_filter_argentina_and_spain_movie_with_only_one_country_returns_false(self):
        movie = Movie(
            movie_id=1,
            title='Random movie',
            production_countries=['Argentina'],
        )

        logic = ArgentinaAndSpainLogic()

        assert not logic.should_pass(movie)


if __name__ == '__main__':
    unittest.main()
