import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.movie_budget_counter import MovieBudgetCounter

from .base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q2Top5BudgetSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.final_budgets = {}
        logger.info('Q2Top5BudgetSinkLogic initialized.')

    def merge_results(self, message: Message):
        movie_budget_counters: list[MovieBudgetCounter] = message.data
        for movie_budget_counter in movie_budget_counters:
            counter = self.final_budgets.get(
                movie_budget_counter.country,
            )

            if counter is None:
                counter = movie_budget_counter
            else:
                counter.total_budget += movie_budget_counter.total_budget

            self.final_budgets[movie_budget_counter.country] = counter

    def message_result(self, user_id: int) -> Message:
        sorted_countries = self._obtain_sorted_countries()
        return Message(user_id, MessageType.MovieBudgetCounter, sorted_countries)

    def _obtain_sorted_countries(self) -> list[MovieBudgetCounter]:
        logger.info('--- Sink: Final Global Country Budget Counts ---')
        if not self.final_budgets:
            logger.info('No country budgets aggregated.')
            return []

        try:
            sorted_countries = sorted(
                self.final_budgets.items(),
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
