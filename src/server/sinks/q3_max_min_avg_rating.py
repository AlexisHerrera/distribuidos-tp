import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_rating_avg import MovieRatingAvg
from src.model.movie_rating_count import MovieRatingCount

from .base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q3MaxMinAvgRatingSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.rating_counts: dict[int, dict[int, MovieRatingCount]] = {}
        logger.info('Q3MaxMinAvgRatingSinkLogic initialized.')

    def merge_results(self, message: Message):
        rating_counts: list[MovieRatingCount] = message.data
        user_id = message.user_id
        partial_result = self.rating_counts.get(user_id, {})

        for rating in rating_counts:
            counter = partial_result.get(rating.movie_id)

            if counter is None:
                counter = rating
            else:
                counter.partial_sum += rating.partial_sum
                counter.count += rating.count

            partial_result[rating.movie_id] = counter

        self.rating_counts[user_id] = partial_result

    def message_result(self, user_id: int) -> Message:
        result = self._get_max_min_average_rating(user_id)
        return Message(user_id, MessageType.MovieRatingAvg, result)

    def _get_max_min_average_rating(self, user_id: int) -> dict:
        logger.info(f'--- Sink: Final Global Q3 Max Min Avg Rating for {user_id} ---')
        result = self.rating_counts.get(user_id, {})
        final_result = {
            'min': None,
            'max': None,
        }

        if not result:
            logger.info('No rating counts aggregated.')
            return final_result
        try:
            for rating in result.values():
                avg = rating.partial_sum / rating.count

                if not final_result['min']:
                    final_result['min'] = MovieRatingAvg(
                        rating.movie_id, rating.title, avg
                    )
                if not final_result['max']:
                    final_result['max'] = MovieRatingAvg(
                        rating.movie_id, rating.title, avg
                    )

                if avg >= final_result['max'].average_rating:
                    final_result['max'] = MovieRatingAvg(
                        rating.movie_id, rating.title, avg
                    )
                if avg <= final_result['min'].average_rating:
                    final_result['min'] = MovieRatingAvg(
                        rating.movie_id, rating.title, avg
                    )

            for k, v in final_result.items():
                logger.info(
                    f'{k} movie average: {v.id} - {v.title} - {v.average_rating}'
                )

            logger.info('---------------------------------------------')

            return final_result
        except Exception as e:
            logger.error(f'Error logging final results: {e}', exc_info=True)
