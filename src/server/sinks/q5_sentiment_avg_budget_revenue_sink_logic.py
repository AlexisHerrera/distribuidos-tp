import logging
import uuid
from typing import Tuple, Any

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_avg_budget import MovieAvgBudget
from src.model.movie_sentiment import MovieSentiment
from src.server.sinks.base_sink_logic import BaseSinkLogic
from src.utils.safe_dict import SafeDict

logger = logging.getLogger(__name__)


class Q5SentimentAvgBudgetRevenueSinkLogic(BaseSinkLogic):
    def __init__(self):
        self._stats = SafeDict()
        logger.info('Q5SentimentAvgBudgetRevenueSinkLogic initialized.')

    def merge_results(self, message: Message) -> None:
        list_movie_sentiments: list[MovieSentiment] = message.data
        user_id = message.user_id
        partial_result: dict[str, dict[str, float]] = self._stats.get(user_id, {})

        for ms in list_movie_sentiments:
            if ms.budget:
                ratio = ms.revenue / ms.budget
                stats = partial_result.get(ms.sentiment, {'sum': 0.0, 'count': 0})
                stats['sum'] += ratio
                stats['count'] += 1
                partial_result[ms.sentiment] = stats
            else:
                logger.warning(f'Movie id={ms.id} tiene budget=0, se omite ratio.')

        self._stats.set(user_id, partial_result)

    def _obtain_avg_budget_revenue(self, user_id: uuid.UUID) -> Tuple[float, float]:
        result: dict[str, dict[str, float]] = self._stats.pop(user_id, {})
        pos = result.get('POSITIVE', {'sum': 0.0, 'count': 0})
        neg = result.get('NEGATIVE', {'sum': 0.0, 'count': 0})

        avg_pos = pos['sum'] / pos['count'] if pos['count'] else 0.0
        avg_neg = neg['sum'] / neg['count'] if neg['count'] else 0.0

        return avg_pos, avg_neg

    def message_result(self, user_id: uuid.UUID) -> Message:
        avg_pos, avg_neg = self._obtain_avg_budget_revenue(user_id)
        logger.info(f'Averages by sentiment - POSITIVE: {avg_pos}, NEGATIVE: {avg_neg}')
        result = MovieAvgBudget(positive=avg_pos, negative=avg_neg)
        return Message(user_id, MessageType.MovieAvgBudget, result, message_id=None)

    def get_application_state(self) -> dict[str, Any]:
        serializable_state = {}
        for user_id, stats_dict in self._stats.to_dict().items():
            serializable_state[str(user_id)] = stats_dict
        return serializable_state

    def load_application_state(self, state: dict[str, Any]) -> None:
        logger.info(
            'Loading application state for Q5SentimentAvgBudgetRevenueSinkLogic...'
        )
        deserialized_state = {}
        for user_id_str, stats_dict in state.items():
            deserialized_state[uuid.UUID(user_id_str)] = stats_dict
        self._stats = SafeDict(initial_dict=deserialized_state)
