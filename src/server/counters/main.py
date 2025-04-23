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
        self._final_results_sent = False

        super().__init__(config, counter_type)

        self.logic: BaseCounterLogic

        logger.info(f"GenericCounterNode '{counter_type}' initialized.")

    def _start_eof_monitor(self):
        if not self.leader.enabled:
            return
        self.leader.wait_for_eof()
        logger.info('EOF detected by monitor')
        self._send_final_results()
        self.shutdown()

    def _send_final_results(self):
        if self._final_results_sent:
            return
        try:
            out_msg = self.logic.message_result()
            self.connection.send(out_msg)
            logger.info('Final counter results sent.')
            self._final_results_sent = True
        except Exception as e:
            logger.error(f'Error sending final counter results: {e}', exc_info=True)

    def shutdown(self):
        self._send_final_results()
        super().shutdown()

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_COUNTER_LOGICS

    def handle_message(self, message: Message):
        if not self.is_running():
            return
        try:
            if message.message_type == MessageType.Movie:
                self.logic.process_message(message)
            else:
                logger.warning(f'Unknown message: {message}')

        except Exception as e:
            logger.error(f'Error processing message in CounterNode: {e}', exc_info=True)


if __name__ == '__main__':
    GenericCounterNode.main(AVAILABLE_COUNTER_LOGICS)
