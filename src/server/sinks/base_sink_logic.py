import logging
from abc import ABC, abstractmethod
from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseSinkLogic(ABC):
    @abstractmethod
    def process_message(self, message: Message):
        pass

    @abstractmethod
    def finalize_and_log(self):
        pass

    @abstractmethod
    def setup(self):
        pass

    def cleanup(self):
        logger.info(f'Cleaning up {type(self).__name__}.')
        pass
