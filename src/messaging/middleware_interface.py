from abc import ABC, abstractmethod

class MiddlewareInterface(ABC):
    @abstractmethod
    def __init__(self, config):
        pass

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def recv(self, callback):
        pass

    @abstractmethod
    def stop(self):
        pass
