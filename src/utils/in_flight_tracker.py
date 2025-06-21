import uuid
import logging
from threading import Lock
from collections import defaultdict

logger = logging.getLogger(__name__)


class InFlightTracker:
    def __init__(self):
        self._counts = defaultdict(int)
        self._lock = Lock()

    def increment(self, user_id: uuid.UUID):
        with self._lock:
            self._counts[user_id] += 1
        logger.debug(
            f'In-flight count for {user_id} incremented to {self._counts[user_id]}'
        )

    def decrement(self, user_id: uuid.UUID):
        with self._lock:
            if user_id in self._counts:
                self._counts[user_id] -= 1
                if self._counts[user_id] == 0:
                    del self._counts[user_id]
                    logger.debug(
                        f'In-flight count for {user_id} is now 0. Key removed.'
                    )
                else:
                    logger.debug(
                        f'In-flight count for {user_id} decremented to {self._counts[user_id]}'
                    )
            else:
                logger.warning(
                    f'Attempted to decrement in-flight count for user_id {user_id}, but it was not being tracked.'
                )

    def get(self, user_id: uuid.UUID) -> int:
        """
        Obtiene el contador actual para un user_id de forma segura.
        """
        with self._lock:
            return self._counts.get(user_id, 0)
