import logging
from abc import ABC, abstractmethod
from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseSinkLogic(ABC):
    @abstractmethod
    def merge_results(self, message: Message):
        pass

    @abstractmethod
    def message_result(self) -> Message:
        pass
