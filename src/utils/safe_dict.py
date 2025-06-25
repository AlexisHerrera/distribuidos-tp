import threading
from typing import Dict, TypeVar

K = TypeVar('K')
V = TypeVar('V')


class SafeDict:
    def __init__(self, initial_dict: Dict[K, V] | None = None):
        self.lock = threading.Lock()
        self.data: Dict[K, V] = initial_dict.copy() if initial_dict else {}

    def get(self, key: K, default: V | None = None) -> V | None:
        with self.lock:
            return self.data.get(key, default)

    def set(self, key: K, value: V):
        with self.lock:
            self.data[key] = value

    def pop(self, key: K, default: V | None = None) -> V | None:
        with self.lock:
            return self.data.pop(key, default)

    def to_dict(self) -> Dict[K, V]:
        with self.lock:
            return self.data.copy()
