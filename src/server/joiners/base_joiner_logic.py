import logging
from abc import ABC, abstractmethod
from typing import Any

from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseJoinerLogic(ABC):
    base_data: dict[int, dict[int, Any]]

    @abstractmethod
    def merge(self, message: Message) -> Any:
        pass
