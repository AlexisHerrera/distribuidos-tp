import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any

from src.messaging.protocol.message import Message
from src.model.movie import Movie

logger = logging.getLogger(__name__)


class BaseJoinerLogic(ABC):
    base_data: dict[uuid.UUID, dict[int, Any]]

    @abstractmethod
    def merge(self, message: Message) -> Any:
        pass

    def get_application_state(self) -> dict[str, Any]:
        serializable_data = {}
        for user_id, movies in self.base_data.items():
            serializable_data[str(user_id)] = {
                movie_id: movie.to_dict() for movie_id, movie in movies.items()
            }
        return serializable_data

    def load_application_state(self, state: dict[str, Any]):
        reconstructed_data = {}
        for user_id_str, movies_dict in state.items():
            user_id = uuid.UUID(user_id_str)
            reconstructed_data[user_id] = {
                int(movie_id): Movie(**movie_data)
                for movie_id, movie_data in movies_dict.items()
            }
        self.base_data = reconstructed_data
        logger.info(f'Restored base data for {len(self.base_data)} users.')
