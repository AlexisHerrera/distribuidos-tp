from middleware_interface import MiddlewareInterface

class Middleware(MiddlewareInterface):
    def __init__(self, config):
        pass

    def send(self, message):
        pass


    def recv(self, message):
        pass


    def stop(self):
        pass