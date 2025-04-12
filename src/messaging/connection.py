from typing import Callable
from src.messaging.broker import Broker
from src.messaging.publisher import Publisher
from src.messaging.consumer import Consumer


class Connection():
    def __init__(self, broker: Broker, publisher: Publisher, consumer: Consumer):
        self.__broker = broker
        self.__publisher = publisher
        self.__consumer = consumer

    def send(self, message):
        # encode message and send
        self.__publisher.put(self.__broker, message)

    def recv(self, callback: Callable):
        def __callback(ch, method, _properties, body):
            try:
                # decode message and pass to callback
                message = body.decode('UTF-8')
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                ch.basic_nack(delivery_tag=method.delivery_tag)

        self.__consumer.consume(self.__broker, __callback)

    def close(self):
        self.__broker.close()
