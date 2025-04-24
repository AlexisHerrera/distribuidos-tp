import logging
from typing import Dict, Type

from src.messaging.protocol.message import Message
from src.server.base_node import BaseNode
from src.server.counters.base_counter_logic import BaseCounterLogic
from src.server.counters.country_budget_logic import CountryBudgetLogic
from src.utils.config import Config

logger = logging.getLogger(__name__)

# Registry of available counter types
AVAILABLE_JOINER_LOGICS: Dict[str, Type[BaseCounterLogic]] = {
    'ratings_joiner': CountryBudgetLogic,
}


class GenericJoinerNode(BaseNode):
    def __init__(self, config: Config, counter_type: str):
        super().__init__(config, counter_type)
        logger.info(f"GenericJoinerNode '{counter_type}' initialized.")
        # TODO load one queue in memory

        self.base_data = self._load_base_data()

    def _load_base_data(self):
        queue_name = self.config.base_db
        logger.info(f'QUEUE BASE NAME: {queue_name}')
        # broker = RabbitMQBroker(self.config.rabbit_host)
        # consumer = NamedQueueConsumer(broker, queue_name)
        # todo: while not message.MessageType == MessageType.EOF. consume else close broker
        data = []
        # broker.close()
        self.base_data = data
        logger.info(f'Data obtained length {len(data)}')

    def handle_message(self, message: Message):
        if not self.is_running():
            return
        try:
            self.logic.process_message(message)
        except Exception as e:
            logger.error(f'Error processing message in JoinerNode: {e}', exc_info=True)

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_JOINER_LOGICS


if __name__ == '__main__':
    GenericJoinerNode.main(AVAILABLE_JOINER_LOGICS)
