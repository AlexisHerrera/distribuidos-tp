import logging
import uuid
from typing import Any

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_rating_avg import MovieRatingAvg
from src.model.movie_rating_count import MovieRatingCount
from src.utils.safe_dict import SafeDict

from .base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q3MaxMinAvgRatingSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.rating_counts = SafeDict()
        logger.info('Q3MaxMinAvgRatingSinkLogic initialized.')

    def merge_results(self, message: Message):
        rating_counts: list[MovieRatingCount] = message.data
        user_id = message.user_id
        partial_result: dict[int, MovieRatingCount] = self.rating_counts.get(
            user_id, {}
        )

        for rating in rating_counts:
            counter = partial_result.get(rating.movie_id)

            if counter is None:
                counter = rating
            else:
                counter.partial_sum += rating.partial_sum
                counter.count += rating.count

            partial_result[rating.movie_id] = counter

        self.rating_counts.set(user_id, partial_result)

    def message_result(self, user_id: uuid.UUID) -> Message:
        result = self._get_max_min_average_rating(user_id)
        return Message(user_id, MessageType.MovieRatingAvg, result, message_id=None)

    def _get_max_min_average_rating(self, user_id: uuid.UUID) -> dict:
        logger.info(f'--- Sink: Final Global Q3 Max Min Avg Rating for {user_id} ---')
        result: dict[int, MovieRatingCount] = self.rating_counts.pop(user_id, {})
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

    def get_application_state(self) -> dict[str, Any]:
        serializable_state = {}
        for user_id, rating_dict in self.rating_counts.to_dict().items():
            serializable_state[str(user_id)] = {
                movie_id: counter.to_dict() for movie_id, counter in rating_dict.items()
            }
        return serializable_state

    def load_application_state(self, state: dict[str, Any]) -> None:
        logger.info('Loading application state for Q3MaxMinAvgRatingSinkLogic...')
        deserialized_state = {}
        for user_id, rating_dict in state.items():
            deserialized_state[uuid.UUID(user_id)] = {
                int(movie_id): MovieRatingCount.from_dict(counter_dict)
                for movie_id, counter_dict in rating_dict.items()
            }
        self.rating_counts = SafeDict(initial_dict=deserialized_state)
