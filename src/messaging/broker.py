from abc import ABC, abstractmethod
from typing import Callable

import pika


class Broker(ABC):
    @abstractmethod
    def exchange_declare(self, exchange_name: str, exchange_type: str):
        pass

    @abstractmethod
    def queue_declare(
        self, queue_name: str = '', exclusive: bool = False, durable: bool = True
    ) -> str:
        pass

    @abstractmethod
    def queue_bind(self, exchange_name: str, queue_name: str):
        pass

    @abstractmethod
    def basic_qos(self):
        pass

    @abstractmethod
    def put(self, exchange: str, routing_key: str, body: bytes):
        pass

    @abstractmethod
    def start_consuming(self, callback: Callable, queue_name: str):
        pass

    @abstractmethod
    def basic_consume(self, callback: Callable, queue_name: str) -> str:
        pass

    @abstractmethod
    def basic_cancel(self, consumer_tag: str):
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

    @abstractmethod
    def process_data_events(self, time_limit: int):
        pass

    @abstractmethod
    def add_callback_threadsafe(self, callback: Callable):
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

    def basic_qos(self):
        self.__channel.basic_qos(prefetch_count=1)

    def put(self, exchange: str = '', routing_key: str = '', body: bytes = b''):
        self.__channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=body
        )

    def start_consuming(self, callback: Callable, queue_name: str):
        self.__channel.start_consuming()

    def basic_consume(self, callback: Callable, queue_name: str) -> str:
        return self.__channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=False
        )

    def basic_get(self, queue_name: str) -> tuple:
        return self.__channel.basic_get(queue=queue_name, auto_ack=False)

    def ack(self, delivery_tag: int):
        self.__channel.basic_ack(delivery_tag=delivery_tag)

    def _threadsafe_teardown(self):
        try:
            if self.__channel.is_open:
                self.__channel.close()
        except Exception:
            pass
        try:
            if self.__connection.is_open:
                self.__connection.close()
        except Exception:
            pass

    def close(self):
        self.__connection.add_callback_threadsafe(self._threadsafe_teardown)

    def basic_cancel(self, consumer_tag: str):
        self.__channel.basic_cancel(consumer_tag)

    def process_data_events(self, time_limit: int):
        self.__connection.process_data_events(time_limit=time_limit)

    def add_callback_threadsafe(self, callback: Callable):
        self.__connection.add_callback_threadsafe(callback)
