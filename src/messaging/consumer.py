import logging
from abc import ABC, abstractmethod
from typing import Callable

import pika
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from src.messaging.broker import Broker, RabbitMQBroker
from src.messaging.protocol.message import Message


class Consumer(ABC):
    @abstractmethod
    def start_consuming(
        self, broker: Broker, callback: Callable[[Message], None]
    ) -> str:
        pass

    @abstractmethod
    def basic_consume(self, broker: Broker, callback: Callable[[Message], None]) -> str:
        pass

    def _create_callback(
        self, callback: Callable[[Message], None], requeue: bool = True
    ):
        def __callback(
            ch: Channel,
            method: Basic.Deliver,
            _properties: BasicProperties,
            body: bytes,
        ):
            delivery_tag = method.delivery_tag if method else None
            processed_successfully = False
            try:
                message = Message.from_bytes(body)
                callback(message)
                processed_successfully = True

            except Exception as e:
                logging.error(
                    f'Error processing message (tag: {delivery_tag}): {e}',
                    exc_info=True,
                )
                processed_successfully = False
            try:
                if not ch.is_open:
                    logging.warning(
                        f'ACK/NACK skipped for tag {delivery_tag}: Channel is closed.'
                    )
                    return

                if processed_successfully:
                    logging.debug(f'Sending ACK for delivery_tag={delivery_tag}')
                    ch.basic_ack(delivery_tag=delivery_tag)
                else:
                    logging.warning(
                        f'Sending NACK (requeue={requeue}) for delivery_tag={delivery_tag} due to processing error.'
                    )
                    ch.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

            except pika.exceptions.ChannelWrongStateError:
                logging.warning(
                    f'Could not ACK/NACK tag {delivery_tag}: Channel closed during/after processing (expected during shutdown).'
                )
            except Exception as e_ack:
                logging.error(
                    f'Unexpected error during ACK/NACK for tag {delivery_tag}: {e_ack}',
                    exc_info=True,
                )

        return __callback


class BroadcastConsumer(Consumer):
    def __init__(self, broker: Broker, exchange_name: str, queue: str):
        broker.exchange_declare(exchange_name, 'fanout')
        self.__queue_name = broker.queue_declare(queue)
        self.tag = None
        broker.queue_bind(exchange_name, self.__queue_name)

    def basic_consume(self, broker: Broker, callback: Callable[[Message], None]) -> str:
        tag = broker.basic_consume(
            self._create_callback(callback=callback, requeue=False), self.__queue_name
        )
        self.tag = tag
        return tag

    def start_consuming(self, broker: Broker, callback: Callable[[Message], None]):
        broker.start_consuming(
            self._create_callback(callback=callback, requeue=False), self.__queue_name
        )


class NamedQueueConsumer(Consumer):
    def __init__(self, broker: RabbitMQBroker, queue_name: str):
        # Declare non-exclusive queue to make it readable from multiple sources (like filters)
        # If it is durable, resists restarts.
        broker.queue_declare(queue_name=queue_name, exclusive=False, durable=True)
        self.__queue_name = queue_name
        self.tag = None

    def basic_consume(self, broker: Broker, callback: Callable[[Message], None]) -> str:
        tag = broker.basic_consume(
            self._create_callback(callback=callback, requeue=False), self.__queue_name
        )
        self.tag = tag
        return tag

    def start_consuming(self, broker: Broker, callback: Callable[[Message], None]):
        broker.start_consuming(
            self._create_callback(callback=callback, requeue=False), self.__queue_name
        )
