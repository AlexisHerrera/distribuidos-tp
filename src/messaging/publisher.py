from abc import ABC, abstractmethod

from src.messaging.broker import Broker
from src.messaging.protocol.message import Message


class Publisher(ABC):
    @abstractmethod
    def put(self, broker: Broker, body):
        pass


class BroadcastPublisher(Publisher):
    def __init__(self, broker: Broker, exchange_name: str):
        broker.exchange_declare(exchange_name, 'fanout')
        self.__exchange_name = exchange_name

    def put(self, broker: Broker, body: Message):
        broker.put(exchange=self.__exchange_name, body=body.to_bytes())

    @property
    def exchange_name(self):
        return self.__exchange_name

class DirectPublisher(Publisher):
    def __init__(self, broker: Broker, queue_name: str):
        broker.queue_declare(queue_name)
        self.__routing_key = queue_name

    def put(self, broker: Broker, body: Message):
        broker.put(routing_key=self.__routing_key, body=body.to_bytes())

    @property
    def routing_key(self):
        return self.__routing_key
