from typing import Callable
from src.messaging.broker import Broker
from src.messaging.protocol.message import Message
from src.messaging.publisher import Publisher
from src.messaging.consumer import Consumer


class Connection:
    def __init__(self, broker: Broker, publisher: Publisher, consumer: Consumer):
        self.__broker = broker
        self.__publisher = publisher
        self.__consumer = consumer

    def send(self, message: Message):
        self.__publisher.put(self.__broker, message)

    def recv(self, callback: Callable[[Message], None]):
        self.__consumer.consume(self.__broker, callback)

    def close(self):
        self.__broker.close()
