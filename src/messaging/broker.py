from abc import ABC, abstractmethod
import pika

class Broker(ABC):
    @abstractmethod
    def exchange_declare(self, exchange_name, exchange_type):
        pass

    @abstractmethod
    def queue_declare(self, queue_name) -> str:
        pass

    @abstractmethod
    def queue_bind(self, exchange_name, queue_name):
        pass

    @abstractmethod
    def put(self, exchange, routing_key, body):
        pass

    @abstractmethod
    def consume(self, callback, queue_name):
        pass

    @abstractmethod
    def close(self):
        pass


class RabbitMQBroker(Broker):
    def __init__(self, host: str):
        self.__connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.__channel = self.__connection.channel()

    def exchange_declare(self, exchange_name, exchange_type):
        self.__channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

    def queue_declare(self, queue_name: str = '') -> str:
        result = self.__channel.queue_declare(queue=queue_name, exclusive=True)
        return result.method.queue

    def queue_bind(self, exchange_name, queue_name):
        self.__channel.queue_bind(exchange=exchange_name, queue=queue_name)

    def put(self, exchange: str = '', routing_key: str = '', body: bytes = b''):
        self.__channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

    def consume(self, callback, queue_name):
        self.__channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        self.__channel.start_consuming()

    def close(self):
        self.__channel.close()
        self.__connection.close()



