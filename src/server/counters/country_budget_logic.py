import logging
import uuid

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_budget_counter import MovieBudgetCounter
from src.utils.safe_dict import SafeDict

from .base_counter_logic import BaseCounterLogic

logger = logging.getLogger(__name__)


class CountryBudgetLogic(BaseCounterLogic):
    def __init__(self):
        self.country_budgets = SafeDict()
        logger.info('CountryBudgetLogic initialized.')

    def process_message(self, message: Message):
        movie_list = message.data
        user_id = message.user_id
        partial_result: dict[str, int] = self.country_budgets.get(user_id, {})

        for movie in movie_list:
            production_countries = movie.production_countries
            country = production_countries[0]
            budget = int(movie.budget)
            partial_result[country] = partial_result.get(country, 0) + budget

        self.country_budgets.set(user_id, partial_result)

    def message_result(self, user_id: uuid.UUID) -> Message:
        user_result = self.country_budgets.pop(user_id, {})

        result = [MovieBudgetCounter(k, v) for k, v in user_result.items()]

        logger.info(f'Se mando: {result}')

        return Message(user_id, MessageType.MovieBudgetCounter, result)

    def log_final_results(self, user_id: uuid.UUID):
        result = self.country_budgets.get(user_id, {})
        logger.info(f'--- Final Country Budget Counts for {user_id} ---')
        if not result:
            logger.info('No country budgets were counted.')
            return

        sorted_countries = sorted(
            result.items(), key=lambda item: item[1], reverse=True
        )

        logger.info('Top 5 Countries by Total Budget Invested (Single Production):')
        for i, (country, total_budget) in enumerate(sorted_countries[:5]):
            logger.info(f'  {i + 1}. {country}: {total_budget}')

        logger.info(f'Total countries counted: {len(sorted_countries)}')
        logger.info('-----------------------------')
