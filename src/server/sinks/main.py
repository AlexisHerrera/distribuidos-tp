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
        self.expected_eof_signals: int = 1
        self.received_eof_count: int = 0
        self.final_result_processed: bool = False

        super().__init__(config, sink_type)
        self.logic: BaseSinkLogic
        logger.info(f"SinkNode '{sink_type}' initialized.")

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_SINK_LOGICS

    def process_message(self, message: Message):
        if not self.is_running():
            return

        try:
            if message.message_type == MessageType.EOF:
                self.received_eof_count += 1
                logger.info(
                    f'EOF Received by SinkNode ({self.received_eof_count}/{self.expected_eof_signals}).'
                )

                if self.received_eof_count >= self.expected_eof_signals:
                    with self.lock:
                        if self.final_result_processed:
                            return
                        logger.info('All expected EOFs received. Finalizing...')
                        self.final_result_processed = True

                    if self.logic:
                        try:
                            self.logic.finalize_and_log()
                        except Exception as e:
                            logger.error(f'Error finalizing sink: {e}', exc_info=True)
                    else:
                        logger.warning('No sink logic loaded for finalization.')
                    self.shutdown()

            elif message.message_type == MessageType.MovieBudgetCounter:
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

    def shutdown(self, force=False):
        if self.logic and hasattr(self.logic, 'cleanup'):
            logger.info('Running sink logic cleanup...')
            try:
                self.logic.cleanup()
            except Exception as e:
                logger.error(f'Sink cleanup error: {e}', exc_info=True)
        super().shutdown(force=force)


if __name__ == '__main__':
    SinkNode.main(AVAILABLE_SINK_LOGICS)
