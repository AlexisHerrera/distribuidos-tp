from threading import Lock


class Runnable:
    def __init__(self, is_running: bool = True):
        self.__lock = Lock()
        self.__is_running = is_running

    def __call__(self) -> bool:
        with self.__lock:
            return self.__is_running

    def stop(self):
        with self.__lock:
            self.__is_running = False
