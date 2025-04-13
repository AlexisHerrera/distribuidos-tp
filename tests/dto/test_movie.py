import unittest

from src.dto.movie import MovieProtocol
# from src.model.movie import Movie


class TestMovieProtocol:
    def test_to_bytes_empty_list(self):
        res = MovieProtocol.to_bytes([])

        expected = MovieProtocol.MSG_ID.to_bytes(1, 'big') + int.to_bytes(0, 2, 'big')

        assert res == expected

    # def test_to_bytes(self):
    #     movie = Movie(movie_id=1, title="Toy Story")

    #     res = MovieProtocol.to_bytes([movie])

    #     msg_id = b'\x01'
    #     msg_len = b'\x00\x10'
    #     attr_id_movie_id = b'\x01'
    #     movie_id = b'\x00\x00\x00\x01'
    #     attr_id_title = b'\x02'
    #     title = b'Toy Story\x00'
    #     expected = msg_id + msg_len + attr_id_movie_id + movie_id + attr_id_title + title

    #     assert res == expected


if __name__ == "__main__":
    unittest.main()
