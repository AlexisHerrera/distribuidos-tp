from abc import ABC, abstractmethod


class Consumer(ABC):
    @abstractmethod
    def consume(self, conn, callback):
        pass


class BroadcastConsumer():
    def __init__(self, conn, exchange_name):
        conn.exchange_declare(exchange_name, 'fanout')
        self.__queue_name = conn.queue_declare()
        conn.queue_bind(exchange_name, self.__queue_name)

    def consume(self, conn, callback):
        conn.consume(callback, self.__queue_name)
