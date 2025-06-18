import logging
from typing import Dict, Type

from src.messaging.protocol.message import Message
from src.server.base_node import BaseNode
from src.server.counters.actor_counter_logic import ActorCounterLogic
from src.server.counters.base_counter_logic import BaseCounterLogic
from src.server.counters.country_budget_logic import CountryBudgetLogic
from src.server.counters.rating_counter_logic import RatingCounterLogic
from src.utils.config import Config

logger = logging.getLogger(__name__)

# Registry of available counter types
AVAILABLE_COUNTER_LOGICS: Dict[str, Type[BaseCounterLogic]] = {
    'country_budget': CountryBudgetLogic,
    'rating': RatingCounterLogic,
    'actor_counter': ActorCounterLogic,
}


class GenericCounterNode(BaseNode):
    def __init__(self, config: Config, counter_type: str):
        super().__init__(config, counter_type)
        logger.info(f"GenericCounterNode '{counter_type}' initialized.")

    def handle_message(self, message: Message):
        if not self.is_running():
            return
        try:
            output_message = self.logic.process_message(message)
            if output_message:
                self.connection.send(output_message)

        except Exception as e:
            logger.error(f'Error processing message in CounterNode: {e}', exc_info=True)

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_COUNTER_LOGICS


if __name__ == '__main__':
    GenericCounterNode.main(AVAILABLE_COUNTER_LOGICS)
