import logging
from collections import defaultdict
from .base_sink_logic import BaseSinkLogic
from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class Q2Top5BudgetSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.final_budgets = defaultdict(int)
        logger.info('Q2Top5BudgetSinkLogic initialized.')

    def merge_results(self, message: Message):
        result_dict = message.data
        # logger.info("Received message:", message.message_type, message.data)
        for country, budget in result_dict.items():
            self.final_budgets[country] += int(budget)
        self.finalize_and_log()

    def finalize_and_log(self):
        logger.info('--- Sink: Final Global Country Budget Counts ---')
        if not self.final_budgets:
            logger.info('No country budgets aggregated.')
            return
        try:
            sorted_countries = sorted(
                self.final_budgets.items(), key=lambda item: item[1], reverse=True
            )
            logger.info('FINAL Top 5 Countries by Total Budget Invested:')
            for i, (country, total_budget) in enumerate(sorted_countries[:5]):
                logger.info(f'  {i + 1}. {country}: {total_budget}')
            logger.info(f'Total countries aggregated: {len(sorted_countries)}')
            logger.info('---------------------------------------------')
        except Exception as e:
            logger.error(f'Error logging final results: {e}', exc_info=True)
