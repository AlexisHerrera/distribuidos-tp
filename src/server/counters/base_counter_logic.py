import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class BaseCounterLogic(ABC):
    @abstractmethod
    def message_result(self) -> Any:
        pass

    def log_final_results(self):
        results = self.message_result()
        logger.info('--- Final Counter Results ---')
        logger.info(f'{type(self).__name__} Results:\n{results}')
        logger.info('-----------------------------')
