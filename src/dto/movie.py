from src.model.movie import Movie


class MovieProtocol:
    MSG_ID = 1
    MSG_ID_SIZE = 1
    ATTRIBUTE_ID_SIZE = 2
    MOVIE_ID_SIZE = 4
    MSG_LEN_SIZE = 2

    @staticmethod
    def to_bytes(movies: list[Movie]) -> bytes:
        msg_id = int.to_bytes(MovieProtocol.MSG_ID, MovieProtocol.MSG_ID_SIZE, 'big')
        msg_len = 0
        movies_encoded = b''

        for movie in movies:
            (movie_encoded, bytes_amount) = MovieProtocol.movie_to_bytes(movie)
            msg_len += bytes_amount
            movies_encoded += movie_encoded

        return msg_id + int.to_bytes(msg_len, MovieProtocol.MSG_LEN_SIZE, 'big') + movies_encoded

    @staticmethod
    def movie_to_bytes(movie: Movie) -> tuple[bytes, int]:
        buf = b''
        buf_len = 0

        for i in range(1, 9):
            buf += int.to_bytes(i, MovieProtocol.ATTRIBUTE_ID_SIZE, 'big')
            buf_len += MovieProtocol.ATTRIBUTE_ID_SIZE

            match i:
                case 1:
                    buf += int.to_bytes(movie.id, MovieProtocol.MOVIE_ID_SIZE, 'big')
                    buf_len += MovieProtocol.MOVIE_ID_SIZE
                case 2:
                    buf += bytes(f"{movie.title}\x00", 'ascii')
                    buf_len += len(movie.title) + 1

        return buf, buf_len

    @staticmethod
    def from_bytes(buf: bytes) -> list[Movie]:
        pass
