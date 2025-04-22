import logging
from typing import Dict, Type

from src.messaging.protocol.message import Message, MessageType
from src.server.base_node import BaseNode
from src.server.counters.base_counter_logic import BaseCounterLogic
from src.server.counters.country_budget_logic import CountryBudgetLogic
from src.utils.config import Config

logger = logging.getLogger(__name__)


AVAILABLE_COUNTER_LOGICS = {
    'country_budget': CountryBudgetLogic,
}


class GenericCounterNode(BaseNode):
    def __init__(self, config: Config, counter_type: str):
        self.final_results_logged = False

        super().__init__(config, counter_type)

        self.logic: BaseCounterLogic

        logger.info(f"GenericCounterNode '{counter_type}' initialized.")

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_COUNTER_LOGICS

    def process_message(self, message: Message):
        if not self.is_running():
            return

        try:
            if message.message_type == MessageType.EOF:
                logger.info('EOF Received by CounterNode. Ignoring EOF')
                return
                with self.lock:
                    if self.final_results_logged:
                        return
                    logger.info('EOF Received by CounterNode. Finalizing results...')
                    self.final_results_logged = True

                if self.logic:
                    try:
                        self.logic.log_final_results()
                    except Exception as logic_err:
                        logger.error(
                            f'Error during final result logging: {logic_err}',
                            exc_info=True,
                        )
                else:
                    logger.warning('No counter logic loaded to process results.')

                self.shutdown()

            elif self.logic:
                self.logic.process_message(message)
                self.logic.log_final_results()
            else:
                logger.warning(
                    f'Received message type {message.message_type} but no logic loaded.'
                )

        except Exception as e:
            logger.error(f'Error processing message in CounterNode: {e}', exc_info=True)


if __name__ == '__main__':
    GenericCounterNode.main(AVAILABLE_COUNTER_LOGICS)
