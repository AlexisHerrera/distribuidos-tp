import unittest

from src.model.movie import Movie
from src.server.filters.decade_00_filter import Decade00Logic


class TestDecade00Filter:
    def test_filter_decade_00_movie_of_2005_returns_true(self):
        movie = Movie(movie_id=1, title='Random movie', release_date='2005-01-01')

        logic = Decade00Logic()

        assert logic.should_pass(movie)

    def test_filter_decade_00_movie_of_2000_returns_true(self):
        movie = Movie(movie_id=1, title='Random movie', release_date='2000-01-01')

        logic = Decade00Logic()

        assert logic.should_pass(movie)

    def test_filter_decade_00_movie_of_2015_returns_false(self):
        movie = Movie(movie_id=1, title='Random movie', release_date='2015-01-01')

        logic = Decade00Logic()

        assert not logic.should_pass(movie)

    def test_filter_decade_00_movie_of_1995_returns_false(self):
        movie = Movie(movie_id=1, title='Random movie', release_date='1995-01-01')

        logic = Decade00Logic()

        assert not logic.should_pass(movie)


if __name__ == '__main__':
    unittest.main()
