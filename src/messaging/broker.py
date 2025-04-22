from abc import ABC, abstractmethod
from typing import Callable

import pika


class Broker(ABC):
    @abstractmethod
    def exchange_declare(self, exchange_name: str, exchange_type: str):
        pass

    @abstractmethod
    def queue_declare(self, queue_name: str) -> str:
        pass

    @abstractmethod
    def queue_bind(self, exchange_name: str, queue_name: str):
        pass

    @abstractmethod
    def put(self, exchange: str, routing_key: str, body: bytes):
        pass

    @abstractmethod
    def consume(self, callback: Callable, queue_name: str) -> str:
        pass

    @abstractmethod
    def stop_consuming(self, consumer_tag: str):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def basic_get(self, queue_name: str) -> tuple:
        pass

    @abstractmethod
    def ack(self, delivery_tag: int):
        pass


class RabbitMQBroker(Broker):
    def __init__(self, host: str):
        self.__connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.__channel = self.__connection.channel()

    def exchange_declare(self, exchange_name: str, exchange_type: str):
        self.__channel.exchange_declare(
            exchange=exchange_name, exchange_type=exchange_type
        )

    def queue_declare(
        self, queue_name: str = '', exclusive: bool = False, durable: bool = True
    ) -> str:
        # Is an exclusive queue (only
        exclusive_actual = exclusive if queue_name else True
        result = self.__channel.queue_declare(
            queue=queue_name, exclusive=exclusive_actual, durable=durable
        )
        return result.method.queue

    def queue_bind(self, exchange_name: str, queue_name: str):
        self.__channel.queue_bind(exchange=exchange_name, queue=queue_name)

    def put(self, exchange: str = '', routing_key: str = '', body: bytes = b''):
        self.__channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=body
        )

    def consume(self, callback: Callable, queue_name: str):
        consumer_tag = self.__channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=False
        )
        try:
            self.__channel.start_consuming()
        except Exception:
            # TODO: Loggear, aqui debería pasar stop_consuming
            pass
        return consumer_tag

    def basic_get(self, queue_name: str) -> tuple:
        return self.__channel.basic_get(queue=queue_name, auto_ack=False)

    def ack(self, delivery_tag: int):
        self.__channel.basic_ack(delivery_tag=delivery_tag)

    def stop_consuming(self, consumer_tag: str):
        if consumer_tag:
            try:
                self.__channel.basic_cancel(consumer_tag=consumer_tag)
            except Exception:
                pass
        try:
            self.__channel.stop_consuming()
        except Exception:
            pass

    def close(self):
        if self.__channel.is_open:
            self.__channel.close()
        if self.__connection.is_open:
            self.__connection.close()
