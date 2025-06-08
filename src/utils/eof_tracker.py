import uuid
from threading import Event, Lock


class EOFTracker:
    TIMEOUT = 5.0

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

    # def adquire(self, user_id: uuid.UUID):
    #     with self.user_lock:
    #         try:
    #             sem = self.users[user_id]
    #             sem.acquire()
    #         except KeyError:
    #             self.users[user_id] = Lock()
    #             self.users[user_id].acquire()
    #
    # def release(self, user_id: uuid.UUID):
    #     with self.user_lock:
    #         try:
    #             sem = self.users[user_id]
    #             sem.release()
    #         except KeyError:
    #             # There is no Semaphore created
    #             return

    # def get_or_create_user_lock(self, user_id: uuid.UUID):
    #     with self.user_lock:
    #         try:
    #             return self.users[user_id]
    #         except KeyError:
    #             self.users[user_id] = Lock()
    #             return self.users[user_id]

    # def add(self, user_id: uuid.UUID):
    #     with self.user_lock:
    #         self.users[user_id] = Semaphore(1)
    #
    # def set(self, user_id: uuid.UUID):
    #     with self.user_lock:
    #         if user_id in self.users:
    #             self.users[user_id].release()
    #
    # def wait(self, user_id: uuid.UUID) -> bool:
    #     with self.user_lock:
    #         sem = self.users[user_id]
    #
    #     result = sem.acquire()
    #
    #     with self.user_lock:
    #         self.users.pop(user_id, None)  # Silently drop
    #
    #     return result
