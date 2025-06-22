import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any

from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseSinkLogic(ABC):
    @abstractmethod
    def merge_results(self, message: Message):
        pass

    @abstractmethod
    def message_result(self, user_id: uuid.UUID) -> Message:
        pass

    @abstractmethod
    def get_application_state(self) -> dict[str, Any]:
        pass

    @abstractmethod
    def load_application_state(self, state: dict[str, Any]) -> None:
        pass
