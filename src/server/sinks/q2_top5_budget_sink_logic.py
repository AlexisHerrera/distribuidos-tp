import logging

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
        logger.info(f'[{user_id}] Processing {len(movie_budget_counters)} budgets')
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

    def message_result(self, user_id: int) -> Message:
        sorted_countries = self._obtain_sorted_countries(user_id)
        return Message(user_id, MessageType.MovieBudgetCounter, sorted_countries)

    def _obtain_sorted_countries(self, user_id: int) -> list[MovieBudgetCounter]:
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
