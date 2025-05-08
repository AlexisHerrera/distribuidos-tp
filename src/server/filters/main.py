import logging
from typing import Dict, Type

from src.messaging.protocol.message import Message, MessageType
from src.server.base_node import BaseNode
from src.server.filters.argentina_filter import ArgentinaLogic
from src.server.filters.argentina_spain_filter import ArgentinaAndSpainLogic
from src.server.filters.base_filter_logic import BaseFilterLogic
from src.server.filters.budget_revenue_filter import BudgetRevenueLogic
from src.server.filters.decade_00_filter import Decade00Logic
from src.server.filters.post_2000_logic import Post2000Logic
from src.server.filters.single_country_logic import SingleCountryLogic
from src.utils.config import Config

logger = logging.getLogger(__name__)
AVAILABLE_FILTER_LOGICS = {
    'solo_country': SingleCountryLogic,
    'post_2000': Post2000Logic,
    'argentina': ArgentinaLogic,
    'argentina_and_spain': ArgentinaAndSpainLogic,
    'decade_00': Decade00Logic,
    'budget_revenue': BudgetRevenueLogic,
}


class FilterNode(BaseNode):
    def __init__(self, config: Config, filter_type: str):
        super().__init__(config, filter_type)

        self.logic: BaseFilterLogic

        logger.info(f"FilterNode '{filter_type}' initialized.")

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_FILTER_LOGICS

    def handle_message(self, message: Message):
        if not self.is_running():
            return

        if message.message_type == MessageType.Movie:
            movies_list = message.data
            if not isinstance(movies_list, list):
                logger.warning(f'Expected list, got {type(movies_list)}')
                return

            movies = []
            for movie in movies_list:
                if not movie:
                    continue

                passed_filter = False

                try:
                    passed_filter = self.logic.should_pass(movie)
                except Exception as e:
                    logger.error(f'Filter logic error: {e}', exc_info=True)

                if passed_filter:
                    new_movie = self.logic.map(movie)
                    movies.append(new_movie)

            if len(movies) > 0:
                try:
                    self.connection.send(
                        Message(message.user_id, MessageType.Movie, movies)
                    )
                    logger.info(f'[{message.user_id}] Se enviaron {len(movies)}')
                except Exception as e:
                    logger.error(
                        f'Error Publishing movies: {e}',
                        exc_info=True,
                    )
        else:
            logger.warning(f'Unknown message type received: {message.message_type}')


if __name__ == '__main__':
    FilterNode.main(AVAILABLE_FILTER_LOGICS)
