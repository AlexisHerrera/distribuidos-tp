import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_rating import MovieRating
from src.model.movie_rating_count import MovieRatingCount
from src.server.counters.base_counter_logic import BaseCounterLogic

logger = logging.getLogger(__name__)


class RatingCounterLogic(BaseCounterLogic):
    def __init__(self):
        logger.info('RatingCounterLogic initialized.')

    def process_message(self, message: Message) -> Message | None:
        movie_rating_list: list[MovieRating] = message.data
        user_id = message.user_id

        batch_result: dict[int, MovieRatingCount] = {}

        for movie in movie_rating_list:
            counter = batch_result.get(movie.movie_id)

            if counter is None:
                counter = MovieRatingCount(movie.movie_id, movie.title, movie.rating, 1)
            else:
                counter.partial_sum += movie.rating
                counter.count += 1

            batch_result[movie.movie_id] = counter

        if not batch_result:
            return None

        result_list = list(batch_result.values())

        logger.info(f'[{user_id}] Sending {len(result_list)} partial rating counts.')
        return Message(
            user_id, MessageType.MovieRatingCounter, result_list, message.message_id
        )
