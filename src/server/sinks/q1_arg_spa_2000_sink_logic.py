import logging
import uuid
from collections import defaultdict
from typing import Any

from src.messaging.protocol.message import Message, MessageType
from src.utils.safe_dict import SafeDict

from ...model.movie import Movie
from .base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q1ArgSpa2000(BaseSinkLogic):
    def __init__(self):
        self.final_movies_and_genres = SafeDict()
        defaultdict()
        logger.info('Q1ArgSpa2000 initialized.')

    def merge_results(self, message: Message):
        list_movies_genres: list[Movie] = message.data
        user_id = message.user_id
        partial_result: list[Movie] = self.final_movies_and_genres.get(user_id, [])

        for movie_genre in list_movies_genres:
            # Asumimos id unico de movie, por lo que no necesito
            # agrupar por id, además que hice que el filtro lo saque
            partial_result.append(movie_genre)

        self.final_movies_and_genres.set(user_id, partial_result)

    def message_result(self, user_id: uuid.UUID) -> Message:
        result = self.final_movies_and_genres.pop(user_id, [])
        # Just to get consistent results
        result.sort(key=lambda movie: movie.id)

        logger.info(f'Final Movie Genre for user {user_id}: count {len(result)}')
        for movies_genre in result:
            logger.info(f'Movie: {movies_genre.title}, Genres: {movies_genre.genres}')
        # In case of duplicates, Cleaner should ignore if sink has already given results for the client
        return Message(user_id, MessageType.Movie, result, message_id=None)

    def get_application_state(self) -> dict[str, Any]:
        serializable_state = {}
        for user_id, movie_list in self.final_movies_and_genres.to_dict().items():
            serializable_state[str(user_id)] = [movie.to_dict() for movie in movie_list]
        return serializable_state

    def load_application_state(self, state: dict[str, Any]) -> None:
        logger.info('Loading application state for Q1ArgSpa2000...')
        deserialized_state = {}
        for user_id, movie_list in state.items():
            deserialized_state[uuid.UUID(user_id)] = [
                Movie.from_dict(movie_dict) for movie_dict in movie_list
            ]
        self.final_movies_and_genres = SafeDict(initial_dict=deserialized_state)
