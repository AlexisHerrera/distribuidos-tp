import logging
from abc import ABC, abstractmethod

from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseCounterLogic(ABC):
    @abstractmethod
    def message_result(self, user_id: int) -> Message:
        pass

    def log_final_results(self, user_id):
        results = self.message_result(user_id)
        logger.info('--- Final Counter Results ---')
        logger.info(f'{type(self).__name__} Results:\n{results}')
        logger.info('-----------------------------')

    @abstractmethod
    def process_message(self, message: Message):
        pass
