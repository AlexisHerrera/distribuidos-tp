import threading


class SafeDict:
    def __init__(self):
        self.lock = threading.Lock()
        self.data = {}

    def get(self, key, default: object = None) -> object:
        with self.lock:
            return self.data.get(key, default)

    def set(self, key, value):
        with self.lock:
            self.data[key] = value

    def pop(self, key, default: object = None) -> object:
        with self.lock:
            return self.data.pop(key, default)
