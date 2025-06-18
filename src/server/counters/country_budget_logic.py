import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_budget_counter import MovieBudgetCounter
from src.utils.safe_dict import SafeDict

from .base_counter_logic import BaseCounterLogic

logger = logging.getLogger(__name__)


class CountryBudgetLogic(BaseCounterLogic):
    def __init__(self):
        self.country_budgets = SafeDict()
        logger.info('CountryBudgetLogic initialized.')

    def process_message(self, message: Message) -> Message | None:
        movie_list = message.data
        user_id = message.user_id

        batch_result: dict[str, int] = {}

        for movie in movie_list:
            if movie.production_countries:
                country = movie.production_countries[0]
                budget = int(movie.budget)
                batch_result[country] = batch_result.get(country, 0) + budget

        if not batch_result:
            return None

        result_list = [MovieBudgetCounter(k, v) for k, v in batch_result.items()]

        logger.info(
            f'Processed batch for user {user_id}, sending {len(result_list)} partial budget counts.'
        )

        return Message(user_id, MessageType.MovieBudgetCounter, result_list)
