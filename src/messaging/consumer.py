from abc import ABC, abstractmethod

from src.messaging.broker import Broker


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
                # message = Movies.from_bytes(body)
                # callback(message)
                callback(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                ch.basic_nack(delivery_tag=method.delivery_tag)


        broker.consume(__callback, self.__queue_name)
