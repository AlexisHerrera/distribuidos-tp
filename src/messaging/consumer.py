from abc import ABC, abstractmethod
import logging

from src.messaging.broker import Broker
from src.messaging.message import Message


class Consumer(ABC):
    @abstractmethod
    def consume(self, broker: Broker, callback):
        pass


class BroadcastConsumer():
    def __init__(self, broker: Broker, exchange_name: str):
        broker.exchange_declare(exchange_name, 'fanout')
        self.__queue_name = broker.queue_declare()
        broker.queue_bind(exchange_name, self.__queue_name)

    def consume(self, broker: Broker, callback):
        def __callback(ch, method, _properties, body):
            try:
                # decode message and pass to callback
                message = Message.from_bytes(body)
                callback(message)
                # callback(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error("%s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag)


        broker.consume(__callback, self.__queue_name)
