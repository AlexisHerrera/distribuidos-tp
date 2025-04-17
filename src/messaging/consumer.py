from abc import ABC, abstractmethod
import logging
from typing import Callable

from src.messaging.broker import Broker, RabbitMQBroker
from src.messaging.broker import Broker
from src.messaging.protocol.message import Message


class Consumer(ABC):
    @abstractmethod
    def consume(self, broker: Broker, callback: Callable[[Message], None]):
        pass


class BroadcastConsumer():
    def __init__(self, broker: Broker, exchange_name: str):
        broker.exchange_declare(exchange_name, 'fanout')
        self.__queue_name = broker.queue_declare()
        broker.queue_bind(exchange_name, self.__queue_name)

    def consume(self, broker: Broker, callback: Callable[[Message], None]):
        broker.consume(self._create_callback(callback), self.__queue_name)

    def _create_callback(self, callback):
        def __callback(ch, method, _properties, body):
            try:
                # decode message and pass to callback
                message = Message.from_bytes(body)
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error("%s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag)

        return __callback


class NamedQueueConsumer(Consumer):
    def __init__(self, broker: RabbitMQBroker, queue_name: str):
        # Declare non-exclusive queue to make it readable from multiple sources (like filters)
        # If it is durable, resists restarts.
        broker.queue_declare(queue_name=queue_name, exclusive=False, durable=True)
        self.__queue_name = queue_name

    def consume(self, broker: RabbitMQBroker, callback: Callable[[Message], None]):
        def __callback_wrapper(ch, method, _properties, body):
            try:
                message = Message.from_bytes(body)
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error("%s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        broker.consume(__callback_wrapper, self.__queue_name)
