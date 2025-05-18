import uuid
from threading import Event, Lock


class EOFTracker:
    TIMEOUT = 5.0

    def __init__(self):
        self.user_lock = Lock()
        self.users: dict[uuid.UUID, Event] = {}

    def add(self, user_id: uuid.UUID):
        with self.user_lock:
            self.users[user_id] = Event()

    def set(self, user_id: uuid.UUID):
        with self.user_lock:
            if user_id in self.users:
                self.users[user_id].set()

    def wait(self, user_id: uuid.UUID) -> bool:
        with self.user_lock:
            event = self.users[user_id]

        result = event.wait(EOFTracker.TIMEOUT)

        with self.user_lock:
            self.users.pop(user_id, None)  # Silently drop

        return result
