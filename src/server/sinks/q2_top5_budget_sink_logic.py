import logging
from collections import defaultdict
from .base_sink_logic import BaseSinkLogic
from src.messaging.protocol.message import Message
from src.model.movie_budget_counter import MovieBudgetCounter

logger = logging.getLogger(__name__)


class Q2Top5BudgetSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.final_budgets = defaultdict(int)
        logger.info('Q2Top5BudgetSinkLogic initialized.')

    def process_message(self, message: Message):
        result_list: list[MovieBudgetCounter] = message.data
        counter_result: MovieBudgetCounter = result_list[0]
        country = getattr(counter_result, 'country', None)
        total_budget = getattr(counter_result, 'total_budget', None)

        if country and isinstance(total_budget, int) and total_budget >= 0:
            logger.debug(f'Received partial budget for {country}: {total_budget}')
            self.final_budgets[country] += total_budget
        else:
            logger.warning(
                f'Received invalid MovieBudgetCounter data: {counter_result.__dict__}. Skipping.'
            )

    def finalize_and_log(self):
        logger.info('--- Sink: Final Global Country Budget Counts ---')
        if not self.final_budgets:
            logger.info('No country budgets were aggregated by the sink.')
            return

        sorted_countries = sorted(
            self.final_budgets.items(),
            key=lambda item: item[1],  # item[1] es el presupuesto total
            reverse=True,
        )

        logger.info(
            'FINAL Top 5 Countries by Total Budget Invested (Single Production):'
        )
        for i, (country, total_budget) in enumerate(sorted_countries[:5]):
            logger.info(f'  {i + 1}. {country}: {total_budget}')

        logger.info(f'Total countries aggregated: {len(sorted_countries)}')
        logger.info('---------------------------------------------')

    # setup() y cleanup() no son necesarios por ahora
