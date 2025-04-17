from abc import ABC, abstractmethod


class Publisher(ABC):
    @abstractmethod
    def put(self, conn, body):
        pass


class BroadcastPublisher():
    def __init__(self, conn, exchange_name):
        conn.exchange_declare(exchange_name, 'fanout')
        self.__exchange_name = exchange_name

    def put(self, conn, body):
        conn.put(exchange=self.__exchange_name, body=body)


class DirectPublisher():
    def __init__(self, conn, queue_name):
        conn.queue_declare(queue_name)
        self.__routing_key = queue_name


    def put(self, conn, body):
        conn.put(routing_key=self.__routing_key, body=body)
