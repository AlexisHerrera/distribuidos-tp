import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_rating import MovieRating
from src.model.movie_rating_count import MovieRatingCount
from src.server.counters.base_counter_logic import BaseCounterLogic

logger = logging.getLogger(__name__)


class RatingCounterLogic(BaseCounterLogic):
    def __init__(self):
        self.ratings: dict[int, MovieRatingCount] = {}
        logger.info('RatingCounterLogic initialized.')

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
        user_id = 1  # TODO: `user_id` will probably be part of `self.actor_counts` and/or needs to be passed as parameter
        result = []
        self.log_final_results()
        for v in self.ratings.values():
            result.append(v)

        return Message(user_id, MessageType.MovieRatingCounter, result)

    def log_final_results(self):
        logger.info('--- Final Rating Counts ---')
        if not self.ratings:
            logger.info('No country budgets were counted.')
            return

        for rating in self.ratings.values():
            logger.info(f'{rating.title} {rating.partial_sum} {rating.count}')

        # logger.info('Top 5 Countries by Total Budget Invested (Single Production):')
        # for i, (country, total_budget) in enumerate(sorted_countries[:5]):
        #     logger.info(f'  {i + 1}. {country}: {total_budget}')

        # logger.info(f'Total countries counted: {len(sorted_countries)}')
        logger.info('-----------------------------')
