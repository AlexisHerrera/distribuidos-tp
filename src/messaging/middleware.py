from src.messaging.middleware_interface import MiddlewareInterface
from src.utils.middleware_config import MiddlewareConfig

class Middleware(MiddlewareInterface):
    def __init__(self, config: MiddlewareConfig):
        pass

    def send(self, message):
        pass


    def recv(self, callback):
        pass


    def stop(self):
        pass
