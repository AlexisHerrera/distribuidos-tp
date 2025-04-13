import logging
from src.model.movie import Movie

# TODO: DO NOT USE to be deleted

class Movies:
    MSG_ID = 1
    MSG_ID_SIZE = 1
    MSG_LEN_SIZE = 2

    def __init__(self, movies: list[Movie]):
        self.__movies = movies

    def to_bytes(self) -> bytes:
        msgId = int.to_bytes(Movies.MSG_ID, Movies.MSG_ID_SIZE, 'big', signed=False)

        encoded_movies = b''
        bytes_amount_movies = 0

        for movie in self.__movies:
            logging.info(f"{movie}")
            (encoded_movie, len_encoded_movie) = self.__movie_to_bytes(movie)
            encoded_movies += encoded_movie
            bytes_amount_movies += len_encoded_movie

        bytes_amount_encoded = int.to_bytes(bytes_amount_movies, Movies.MSG_LEN_SIZE, 'big', signed=False)

        return msgId + bytes_amount_encoded + encoded_movies


    def __movie_to_bytes(self, movie: Movie) -> bytes:
        res = b''

        for attribute_id in range(1, 9):
            attribute_id_encoded = int.to_bytes(attribute_id, 1, 'big', signed=False)
            data = b''

            if attribute_id == 1:
                data = int.to_bytes(movie.id, 1, 'big', signed=False)
            elif attribute_id == 2:
                data = bytes(movie.title, 'utf-8')

            res += attribute_id_encoded + data

        return res, len(data)

    @staticmethod
    def from_bytes(buf: bytes) -> list[Movie]:
        movies = []
        i = 0
        _ = int.from_bytes(buf[i:Movies.MSG_ID_SIZE], 'big', signed=False)

        i = Movies.MSG_ID_SIZE

        msgLen = int.from_bytes(buf[i:i+Movies.MSG_LEN_SIZE], 'big', signed=False)

        j = i + Movies.MSG_LEN_SIZE

        while j < msgLen:
            movie, last_pos = Movies.__movie_from_bytes(buf[j:], msgLen)

            movies.append(movie)
            j += last_pos

        return movies

    @staticmethod
    def __movie_from_bytes(buf: bytes, buf_len: int):
        movie_id = 0
        title = ''
        i = 0
        is_new_movie = False

        while i < buf_len and not is_new_movie:
            attributeId = int.from_bytes(buf[i], 'big', signed=False)
            i += 1

            match attributeId:
                case 1:
                    if movie_id != '':
                        movie_id = int.from_bytes(buf[i:4], 'big')
                    else:
                        is_new_movie = True
                case 2:
                    while buf[i] != '\0':
                        title += buf[i]
                        i += 1


        return Movie(movie_id, title), i
