import uuid
from threading import Event, Lock


class EOFTracker:
    def __init__(self):
        self.user_lock = Lock()
        self.users: dict[uuid.UUID, (bool, Event | None)] = {}

    def processing(self, user_id: uuid.UUID):
        with self.user_lock:
            self.users[user_id] = (True, Event())

    def done(self, user_id: uuid.UUID):
        with self.user_lock:
            (_, event) = self.users[user_id]
            event.set()
            self.users[user_id] = (False, None)

    def wait(self, user_id: uuid.UUID):
        with self.user_lock:
            (processing, event) = self.users.get(user_id, (False, None))

        if processing:
            event.wait()
