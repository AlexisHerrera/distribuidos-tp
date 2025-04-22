import unittest

from src.model.movie import Movie
from src.server.filters.post_2000_logic import Post2000Logic


class TestPost2000Filter:
    def test_filter_post_2000_movie_of_2010_returns_true(self):
        movie = Movie(movie_id=1, title='Random movie', release_date='2010-01-01')

        logic = Post2000Logic()

        assert logic.should_pass(movie)

    def test_filter_post_2000_movie_of_2000_returns_true(self):
        movie = Movie(movie_id=1, title='Random movie', release_date='2000-01-01')

        logic = Post2000Logic()

        assert logic.should_pass(movie)

    def test_filter_post_2000_movie_of_1998_returns_false(self):
        movie = Movie(movie_id=1, title='Random movie', release_date='1998-01-01')

        logic = Post2000Logic()

        assert not logic.should_pass(movie)


if __name__ == '__main__':
    unittest.main()
