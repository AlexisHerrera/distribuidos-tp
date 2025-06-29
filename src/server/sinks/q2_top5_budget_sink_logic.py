import logging
import uuid
from typing import Any

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_budget_counter import MovieBudgetCounter
from src.utils.safe_dict import SafeDict

from .base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q2Top5BudgetSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.final_budgets = SafeDict()
        logger.info('Q2Top5BudgetSinkLogic initialized.')

    def merge_results(self, message: Message):
        movie_budget_counters: list[MovieBudgetCounter] = message.data
        user_id = message.user_id
        partial_result: dict[str, MovieBudgetCounter] = self.final_budgets.get(
            user_id, {}
        )

        for movie_budget_counter in movie_budget_counters:
            counter = partial_result.get(
                movie_budget_counter.country,
            )

            if counter is None:
                counter = movie_budget_counter
            else:
                counter.total_budget += movie_budget_counter.total_budget

            partial_result[movie_budget_counter.country] = counter

        self.final_budgets.set(user_id, partial_result)

    def message_result(self, user_id: uuid.UUID) -> Message:
        sorted_countries = self._obtain_sorted_countries(user_id)
        return Message(
            user_id, MessageType.MovieBudgetCounter, sorted_countries, message_id=None
        )

    def _obtain_sorted_countries(self, user_id: uuid.UUID) -> list[MovieBudgetCounter]:
        result: dict[str, MovieBudgetCounter] = self.final_budgets.pop(user_id, {})
        logger.info(f'--- Sink: Final Global Country Budget Counts for {user_id} ---')
        if not result:
            logger.info('No country budgets aggregated.')
            return []

        try:
            sorted_countries = sorted(
                result.items(),
                key=lambda item: item[1].total_budget,
                reverse=True,
            )

            top5 = [v for (_, v) in sorted_countries[:5]]

            logger.info('FINAL Top 5 Countries by Total Budget Invested:')

            for i, movie_budget_counter in enumerate(top5):
                country = movie_budget_counter.country
                total_budget = movie_budget_counter.total_budget
                logger.info(f'  {i + 1}. {country}: {total_budget}')
            logger.info(f'Total countries aggregated: {len(sorted_countries)}')
            logger.info('---------------------------------------------')

            return top5
        except Exception as e:
            logger.error(f'Error logging final results: {e}', exc_info=True)
            return None

    def get_application_state(self) -> dict[str, Any]:
        serializable_state = {}
        for user_id, budget_dict in self.final_budgets.to_dict().items():
            serializable_state[str(user_id)] = {
                country: counter.to_dict() for country, counter in budget_dict.items()
            }
        return serializable_state

    def load_application_state(self, state: dict[str, Any]) -> None:
        logger.info('Loading application state for Q2Top5BudgetSinkLogic...')
        deserialized_state = {}
        for user_id, budget_dict in state.items():
            deserialized_state[uuid.UUID(user_id)] = {
                country: MovieBudgetCounter.from_dict(counter_dict)
                for country, counter_dict in budget_dict.items()
            }
        self.final_budgets = SafeDict(initial_dict=deserialized_state)
