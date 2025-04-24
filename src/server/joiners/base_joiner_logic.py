import logging
from abc import ABC, abstractmethod
from typing import Any

from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseJoinerLogic(ABC):
    @abstractmethod
    def process_message(self, message: Message) -> Any:
        pass
