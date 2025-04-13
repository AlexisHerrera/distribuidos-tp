from abc import ABC, abstractmethod

from src.messaging.broker import Broker


class Publisher(ABC):
    @abstractmethod
    def put(self, broker, body):
        pass


class BroadcastPublisher():
    def __init__(self, broker: Broker, exchange_name: str):
        broker.exchange_declare(exchange_name, 'fanout')
        self.__exchange_name = exchange_name

    def put(self, broker: Broker, body):
        broker.put(exchange=self.__exchange_name, body=body)


class DirectPublisher():
    def __init__(self, broker: Broker, queue_name: str):
        broker.queue_declare(queue_name)
        self.__routing_key = queue_name


    def put(self, broker: Broker, body):
        broker.put(routing_key=self.__routing_key, body=body)
