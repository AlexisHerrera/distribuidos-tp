import uuid
from threading import Condition


class EOFTracker:
    def __init__(self):
        self.users_condition = Condition()
        # (is_processing, ok)
        # is_processing: True if there's a message of the user being processed
        # ok: if False the process didn't terminate successfully,
        #     True otherwise
        self.users: dict[uuid.UUID, (bool, bool)] = {}

    def processing(self, user_id: uuid.UUID):
        with self.users_condition:
            self.users[user_id] = (True, True)
            self.users_condition.notify_all()

    def done(self, user_id: uuid.UUID, ok: bool = True):
        with self.users_condition:
            self.users[user_id] = (False, ok)
            self.users_condition.notify_all()

    def wait(self, user_id: uuid.UUID):
        with self.users_condition:
            while not self.__user_finished(user_id):
                self.users_condition.wait()

    def __user_finished(self, user_id):
        (processing, ok) = self.users.get(user_id, (False, True))
        return not processing and ok
