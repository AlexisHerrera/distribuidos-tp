import logging
from typing import Dict, Type
from src.messaging.protocol.message import Message, MessageType
from src.messaging.publisher import DirectPublisher
from src.utils.config import Config

from src.server.base_node import BaseNode
from src.server.filters.base_filter_logic import BaseFilterLogic
from src.server.filters.single_country_logic import SingleCountryLogic

logger = logging.getLogger(__name__)
AVAILABLE_FILTER_LOGICS = {
    'solo_country': SingleCountryLogic,
}


class FilterNode(BaseNode):
    def __init__(self, config: Config, filter_type: str):
        self.publisher: DirectPublisher | None = None
        self.output_queue: str | None = None

        super().__init__(config, filter_type)

        self.logic: BaseFilterLogic

        logger.info(f"FilterNode '{filter_type}' initialized.")

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_FILTER_LOGICS

    def process_message(self, message: Message):
        if not self.is_running():
            return

        if message.message_type == MessageType.Movie:
            movies_list = message.data
            if not isinstance(movies_list, list):
                logger.warning(f'Expected list, got {type(movies_list)}')
                return

            for movie in movies_list:
                if not movie:
                    continue
                passed_filter = False
                try:
                    passed_filter = self.logic.should_pass(movie)
                except Exception as e:
                    logger.error(f'Filter logic error: {e}', exc_info=True)

                if passed_filter:
                    try:
                        if not self.publisher:
                            logger.error('Publisher missing!')
                            continue
                        output_message = Message(MessageType.Movie, [movie])
                        logger.info('Movie passed filter!')
                        self.publisher.put(self.broker, output_message)
                    except Exception as e:
                        logger.error(
                            f'Error Publishing ID={getattr(movie, "id", "N/A")}: {e}',
                            exc_info=True,
                        )
        else:
            logger.warning(f'Unknown message type received: {message.message_type}')


if __name__ == '__main__':
    FilterNode.main(AVAILABLE_FILTER_LOGICS)
