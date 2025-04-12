from abc import ABC, abstractmethod

from src.messaging.broker import Broker


class Consumer(ABC):
    @abstractmethod
    def consume(self, broker, callback):
        pass


class BroadcastConsumer():
    def __init__(self, broker: Broker, exchange_name: str):
        broker.exchange_declare(exchange_name, 'fanout')
        self.__queue_name = broker.queue_declare()
        broker.queue_bind(exchange_name, self.__queue_name)

    def consume(self, broker, callback):
        broker.consume(callback, self.__queue_name)
