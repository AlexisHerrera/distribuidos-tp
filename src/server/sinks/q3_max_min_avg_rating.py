import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_rating_avg import MovieRatingAvg
from src.model.movie_rating_count import MovieRatingCount

from .base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q3MaxMinAvgRatingSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.rating_counts: dict[int, MovieRatingCount] = {}
        logger.info('Q3MaxMinAvgRatingSinkLogic initialized.')

    def merge_results(self, message: Message):
        rating_counts: list[MovieRatingCount] = message.data

        for rating in rating_counts:
            counter = self.rating_counts.get(rating.movie_id)

            if counter is None:
                counter = rating
            else:
                counter.partial_sum += rating.partial_sum
                counter.count += rating.count

            self.rating_counts[rating.movie_id] = counter

    def message_result(self, user_id: int) -> Message:
        # user_id = 1  # TODO: `user_id` will probably be part of `self.actor_counts` and/or needs to be passed as parameter
        result = self._get_max_min_average_rating()
        return Message(user_id, MessageType.MovieRatingAvg, result)

    def _get_max_min_average_rating(self) -> dict:
        logger.info('--- Sink: Final Global Q3 Max Min Avg Rating ---')
        if not self.rating_counts:
            logger.info('No rating counts aggregated.')
            return
        try:
            result = {
                'min': MovieRatingAvg(-1, '', 6),
                'max': MovieRatingAvg(-1, '', 0),
            }

            for rating in self.rating_counts.values():
                avg = rating.partial_sum / rating.count
                if avg >= result['max'].average_rating:
                    result['max'] = MovieRatingAvg(rating.movie_id, rating.title, avg)
                if avg <= result['min'].average_rating:
                    result['min'] = MovieRatingAvg(rating.movie_id, rating.title, avg)

            for k, v in result.items():
                logger.info(
                    f'{k} movie average: {v.id} - {v.title} - {v.average_rating}'
                )

            logger.info('---------------------------------------------')

            return result
        except Exception as e:
            logger.error(f'Error logging final results: {e}', exc_info=True)
