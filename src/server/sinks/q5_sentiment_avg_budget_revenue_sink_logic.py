import logging
from collections import defaultdict
from typing import Tuple

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_avg_budget import MovieAvgBudget
from src.model.movie_sentiment import MovieSentiment
from src.server.sinks.base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q5SentimentAvgBudgetRevenueSinkLogic(BaseSinkLogic):
    def __init__(self):
        self._stats: dict[str, dict[str, float]] = defaultdict(
            lambda: {'sum': 0.0, 'count': 0}
        )
        logger.info('Q5SentimentAvgBudgetRevenueSinkLogic initialized.')

    def merge_results(self, message: Message) -> None:
        list_movie_sentiments: list[MovieSentiment] = message.data
        for ms in list_movie_sentiments:
            if ms.budget:
                ratio = ms.revenue / ms.budget
                stats = self._stats[ms.sentiment]
                stats['sum'] += ratio
                stats['count'] += 1
            else:
                logger.warning(f'Movie id={ms.id} tiene budget=0, se omite ratio.')

    def _obtain_avg_budget_revenue(self) -> Tuple[float, float]:
        pos = self._stats.get('POSITIVE', {'sum': 0.0, 'count': 0})
        neg = self._stats.get('NEGATIVE', {'sum': 0.0, 'count': 0})
        avg_pos = pos['sum'] / pos['count'] if pos['count'] else 0.0
        avg_neg = neg['sum'] / neg['count'] if neg['count'] else 0.0
        return avg_pos, avg_neg

    def message_result(self) -> Message:
        user_id = 1  # TODO: `user_id` will probably be part of `self.actor_counts` and/or needs to be passed as parameter
        avg_pos, avg_neg = self._obtain_avg_budget_revenue()
        logger.info(f'Averages by sentiment - POSITIVE: {avg_pos}, NEGATIVE: {avg_neg}')
        result = MovieAvgBudget(positive=avg_pos, negative=avg_neg)
        return Message(user_id, MessageType.MovieAvgBudget, result)
