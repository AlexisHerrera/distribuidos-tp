import logging

from src.messaging.protocol.message import Message, MessageType

from ...model.movie import Movie
from .base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q1ArgSpa2000(BaseSinkLogic):
    def __init__(self):
        self.final_movies_and_genres: dict[int, list[Movie]] = {}
        logger.info('Q1ArgSpa2000 initialized.')

    def merge_results(self, message: Message):
        list_movies_genres: list[Movie] = message.data
        user_id = message.user_id
        partial_result = self.final_movies_and_genres.get(user_id, [])

        for movie_genre in list_movies_genres:
            # Asumimos id unico de movie, por lo que no necesito
            # agrupar por id, además que hice que el filtro lo saque
            partial_result.append(movie_genre)

        self.final_movies_and_genres[user_id] = partial_result

    def message_result(self, user_id: int) -> Message:
        result = self.final_movies_and_genres.pop(user_id, [])

        logger.info(f'Final Movie Genre for user {user_id}: count {len(result)}')
        for movies_genre in result:
            logger.info(f'Movie: {movies_genre.title}, Genres: {movies_genre.genres}')

        return Message(user_id, MessageType.Movie, result)
