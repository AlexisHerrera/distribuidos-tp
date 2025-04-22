import unittest

from src.model.movie import Movie
from src.server.filters.argentina_filter import ArgentinaLogic


class TestArgentinaFilter:
    def test_filter_argentina_movie_with_argentina_returns_true(self):
        movie = Movie(
            movie_id=1,
            title='Random movie',
            production_countries=['Argentina', 'España'],
        )

        logic = ArgentinaLogic()

        assert logic.should_pass(movie)

    def test_filter_argentina_movie_without_argentina_returns_false(self):
        movie = Movie(
            movie_id=1,
            title='Random movie',
            production_countries=['Brasil'],
        )

        logic = ArgentinaLogic()

        assert not logic.should_pass(movie)

    # def test_filter_post_2000_movie_of_2000_returns_true(self):
    #     movie = Movie(movie_id=1, title='Random movie', release_date='2000-01-01')

    #     logic = Post2000Logic()

    #     assert logic.should_pass(movie)

    # def test_filter_post_2000_movie_of_1998_returns_false(self):
    #     movie = Movie(movie_id=1, title='Random movie', release_date='1998-01-01')

    #     logic = Post2000Logic()

    #     assert not logic.should_pass(movie)


if __name__ == '__main__':
    unittest.main()
