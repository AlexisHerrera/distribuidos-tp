import logging
from collections import defaultdict

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_rating import MovieRating
from src.model.movie_rating_count import MovieRatingCount
from src.server.counters.base_counter_logic import BaseCounterLogic

logger = logging.getLogger(__name__)


class RatingCounterLogic(BaseCounterLogic):
    def __init__(self):
        self.ratings = defaultdict(MovieRatingCount)
        logger.info('CountryBudgetLogic initialized.')

    def process_message(self, message: Message):
        movie_rating: list[MovieRating] = message.data
        for movie in movie_rating:
            counter = self.ratings.get(
                movie.movie_id,
            )

            if counter is None:
                counter = MovieRatingCount(movie.movie_id, movie.title, movie.rating, 1)
            else:
                counter.partial_sum += movie.rating
                counter.count += 1

            self.ratings[movie.movie_id] = counter

    def message_result(self) -> Message:
        result = []
        for v in self.ratings.values():
            result.append(v)

        return Message(MessageType.MovieRatingCounter, result)
