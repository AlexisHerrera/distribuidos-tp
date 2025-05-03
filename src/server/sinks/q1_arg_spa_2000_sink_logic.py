import logging

from src.messaging.protocol.message import Message, MessageType

from ...model.movie import Movie
from .base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q1ArgSpa2000(BaseSinkLogic):
    def __init__(self):
        self.final_movies_and_genres: list[Movie] = []
        logger.info('Q1ArgSpa2000 initialized.')

    def merge_results(self, message: Message):
        list_movies_genres: list[Movie] = message.data
        for movie_genre in list_movies_genres:
            # Asumimos id unico de movie, por lo que no necesito
            # agrupar por id, además que hice que el filtro lo saque
            self.final_movies_and_genres.append(movie_genre)

    def message_result(self, user_id: int) -> Message:
        # final_movies_genre = self.final_movies_and_genres
        # user_id = 1  # TODO: `user_id` will probably be part of `self.actor_counts` and/or needs to be passed as parameter
        logger.info(f'Final Movie Genre: count {len(self.final_movies_and_genres)}')
        for movies_genre in self.final_movies_and_genres:
            logger.info(f'Movie: {movies_genre.title}, Genres: {movies_genre.genres}')
        return Message(user_id, MessageType.Movie, self.final_movies_and_genres)
