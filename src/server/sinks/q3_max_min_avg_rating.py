import logging

from src.messaging.protocol.message import Message
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

        self.finalize_and_log()

    def finalize_and_log(self):
        logger.info('--- Sink: Final Global Q3 Max Min Avg Rating ---')
        if not self.rating_counts:
            logger.info('No rating counts aggregated.')
            return
        try:
            min_movie = MovieRatingAvg(-1, '', 5)
            max_movie = MovieRatingAvg(-1, '', 0)
            for rating in self.rating_counts.values():
                avg = rating.partial_sum / rating.count
                if avg > max_movie.average_rating:
                    max_movie = MovieRatingAvg(rating.movie_id, rating.title, avg)
                if avg < min_movie.average_rating:
                    min_movie = MovieRatingAvg(rating.movie_id, rating.title, avg)

            logger.info(
                f'Max movie average: {max_movie.id} - {max_movie.title} - {max_movie.average_rating}'
            )
            logger.info(
                f'Min movie average: {min_movie.id} - {min_movie.title} - {min_movie.average_rating}'
            )

            logger.info('---------------------------------------------')
        except Exception as e:
            logger.error(f'Error logging final results: {e}', exc_info=True)
