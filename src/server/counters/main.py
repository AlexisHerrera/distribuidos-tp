import logging
from typing import Dict, Type

from src.messaging.broker import RabbitMQBroker
from src.messaging.protocol.message import Message
from src.messaging.publisher import DirectPublisher
from src.server.base_node import BaseNode
from src.server.counters.actor_counter_logic import ActorCounterLogic
from src.server.counters.base_counter_logic import BaseCounterLogic
from src.server.counters.country_budget_logic import CountryBudgetLogic
from src.utils.config import Config

logger = logging.getLogger(__name__)

# Registry of available counter types
AVAILABLE_COUNTER_LOGICS: Dict[str, Type[BaseCounterLogic]] = {
    'country_budget': CountryBudgetLogic,
    'actor_counter': ActorCounterLogic,
}


class GenericCounterNode(BaseNode):
    def __init__(self, config: Config, counter_type: str):
        super().__init__(config, counter_type)
        self._final_results_sent = False
        logger.info(f"GenericCounterNode '{counter_type}' initialized.")

    def _send_final_results(self):
        if self._final_results_sent:
            return
        try:
            out_msg = self.logic.message_result()
            broker = RabbitMQBroker(self.config.rabbit_host)
            publisher = DirectPublisher(broker, self.config.publishers[0]['queue'])
            publisher.put(broker, out_msg)
            broker.close()
            logger.info('Final counter results sent (result connection).')
            self._final_results_sent = True
        except Exception as e:
            logger.error(f'Error sending final counter results: {e}', exc_info=True)

    def handle_message(self, message: Message):
        if not self.is_running():
            return
        try:
            self.logic.process_message(message)
        except Exception as e:
            logger.error(f'Error processing message in CounterNode: {e}', exc_info=True)

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_COUNTER_LOGICS


if __name__ == '__main__':
    GenericCounterNode.main(AVAILABLE_COUNTER_LOGICS)
