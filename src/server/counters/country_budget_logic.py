import logging
from collections import defaultdict
from .base_counter_logic import BaseCounterLogic
from src.messaging.protocol.message import Message
from src.model.movie import Movie

logger = logging.getLogger(__name__)


class CountryBudgetLogic(BaseCounterLogic):
    def __init__(self):
        self.country_budgets = defaultdict(int)
        logger.info('CountryBudgetLogic initialized.')

    def process_message(self, message: Message):
        # TODO: Recibir un dto con los datos necesarios solamente
        movie_list = message.data
        movie: Movie = movie_list[0]
        production_countries = movie.production_countries
        country = production_countries[0]
        budget = int(movie.budget)
        self.country_budgets[country] += budget
        # self.log_final_results()

    def get_results(self) -> dict:
        return dict(self.country_budgets)

    def log_final_results(self):
        logger.info('--- Final Country Budget Counts ---')
        if not self.country_budgets:
            logger.info('No country budgets were counted.')
            return

        sorted_countries = sorted(
            self.country_budgets.items(), key=lambda item: item[1], reverse=True
        )

        logger.info('Top 5 Countries by Total Budget Invested (Single Production):')
        for i, (country, total_budget) in enumerate(sorted_countries[:5]):
            logger.info(f'  {i + 1}. {country}: {total_budget}')

        logger.info(f'Total countries counted: {len(sorted_countries)}')
        logger.info('-----------------------------')
