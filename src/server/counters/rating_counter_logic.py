import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_rating import MovieRating
from src.model.movie_rating_count import MovieRatingCount
from src.server.counters.base_counter_logic import BaseCounterLogic
from src.utils.safe_dict import SafeDict

logger = logging.getLogger(__name__)


class RatingCounterLogic(BaseCounterLogic):
    def __init__(self):
        self.ratings = SafeDict()
        logger.info('RatingCounterLogic initialized.')

    def process_message(self, message: Message):
        movie_rating: list[MovieRating] = message.data
        user_id = message.user_id
        partial_result: dict[int, MovieRatingCount] = self.ratings.get(user_id, {})

        for movie in movie_rating:
            counter = partial_result.get(
                movie.movie_id,
            )

            if counter is None:
                counter = MovieRatingCount(movie.movie_id, movie.title, movie.rating, 1)
            else:
                counter.partial_sum += movie.rating
                counter.count += 1

            partial_result[movie.movie_id] = counter

        self.ratings.set(user_id, partial_result)

    def message_result(self, user_id: int) -> Message:
        self.log_final_results(user_id)
        user_result = self.ratings.pop(user_id, {})
        result = []
        for v in user_result.values():
            result.append(v)

        return Message(user_id, MessageType.MovieRatingCounter, result)

    def log_final_results(self, user_id: int):
        result: dict[int, MovieRatingCount] = self.ratings.get(user_id, {})
        logger.info(f'--- Final Rating Counts for {user_id} ---')
        if not result:
            logger.info('No rating were counted.')
            return

        for rating in result.values():
            logger.info(f'{rating.title} {rating.partial_sum} {rating.count}')

        logger.info('-----------------------------')
