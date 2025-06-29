import logging
from abc import ABC, abstractmethod

from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseCounterLogic(ABC):
    @abstractmethod
    def process_message(self, message: Message) -> Message | None:
        pass
