import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.movie import Movie
from src.model.movie_rating import MovieRating
from src.model.rating import Rating
from src.server.joiners.base_joiner_logic import BaseJoinerLogic

logger = logging.getLogger(__name__)


class RatingsJoinerLogic(BaseJoinerLogic):
    def __init__(self):
        # se inyecta desde GenericJoinerNode._thread_load_base
        self.base_data: dict[int, dict[int, Movie]] = {}
        self.batch_count = 0
        logger.info('RatingsJoinerLogic initialized.')

    def merge(self, message: Message) -> Message:
        ratings: list[Rating] = message.data
        joined = []
        for rating in ratings:
            movie = self.base_data.get(message.user_id).get(rating.movie_id)
            if movie is None:
                # logger.warning(f'No movie in base_data for id={rating.movie_id}')
                continue
            joined.append(
                MovieRating(
                    movie_id=rating.movie_id, title=movie.title, rating=rating.rating
                )
            )
        # logger.info(f'Joining {len(joined)}')
        self.batch_count += 1
        logger.info(f'Batch count: {self.batch_count}')
        return Message(
            message.user_id, MessageType.MovieRating, joined, message.message_id
        )
