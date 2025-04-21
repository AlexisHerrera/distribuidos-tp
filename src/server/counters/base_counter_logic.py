import logging
from abc import ABC, abstractmethod
from typing import Any
from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseCounterLogic(ABC):
    @abstractmethod
    def process_message(self, message: Message):
        pass

    @abstractmethod
    def get_results(self) -> Any:
        pass

    def log_final_results(self):
        results = self.get_results()
        logger.info('--- Final Counter Results ---')
        logger.info(f'{type(self).__name__} Results:\n{results}')
        logger.info('-----------------------------')
