import logging
from typing import Dict, Type

from src.messaging.protocol.message import Message, MessageType
from src.server.base_node import BaseNode
from src.server.sinks.base_sink_logic import BaseSinkLogic
from src.server.sinks.q2_top5_budget_sink_logic import Q2Top5BudgetSinkLogic
from src.utils.config import Config

logger = logging.getLogger(__name__)

AVAILABLE_SINK_LOGICS = {
    'q2': Q2Top5BudgetSinkLogic,
}


class SinkNode(BaseNode):
    def __init__(self, config: Config, sink_type: str):
        super().__init__(config, sink_type)
        self.logic: BaseSinkLogic
        logger.info(f"SinkNode '{sink_type}' initialized.")

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_SINK_LOGICS

    def process_message(self, message: Message):
        try:
            if message.message_type == MessageType.MovieBudgetCounter:
                if self.logic:
                    self.logic.process_message(message)
                else:
                    logger.warning('Sink received result but no logic loaded.')
            else:
                logger.debug(
                    f'SinkNode received unhandled type: {message.message_type}'
                )

        except Exception as e:
            logger.error(f'Error processing message in SinkNode: {e}', exc_info=True)


if __name__ == '__main__':
    SinkNode.main(AVAILABLE_SINK_LOGICS)
