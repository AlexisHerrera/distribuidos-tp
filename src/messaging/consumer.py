import logging
from abc import ABC, abstractmethod
from typing import Callable

import pika
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from src.messaging.broker import Broker, RabbitMQBroker
from src.messaging.protocol.message import Message
from src.server.leader_election import FinalizationTimeoutError


class Consumer(ABC):
    def __init__(self, broker: Broker):
        self.tag = None

        self.__queue_name: str = self._setup_topology(broker)
        self._requeue_on_error: bool = self._get_requeue_policy()
        broker.basic_qos()

    @abstractmethod
    def _setup_topology(self, broker: Broker) -> str:
        pass

    @abstractmethod
    def _get_requeue_policy(self) -> bool:
        pass

    def start_consuming(
        self, broker: Broker, callback: Callable[[Message], None]
    ) -> None:
        internal_callback = self._create_callback(
            callback=callback, requeue=self._requeue_on_error
        )
        broker.start_consuming(internal_callback, self.__queue_name)

    def basic_consume(self, broker: Broker, callback: Callable[[Message], None]) -> str:
        internal_callback = self._create_callback(
            callback=callback, requeue=self._requeue_on_error
        )
        tag = broker.basic_consume(internal_callback, self.__queue_name)
        self.tag = tag
        return tag

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
            should_requeue = requeue

            try:
                message = Message.from_bytes(body)
                callback(message)
                processed_successfully = True

            except FinalizationTimeoutError as e:
                logging.error(
                    f'Leader/Follower timeout error (tag: {delivery_tag}): {e}',
                    exc_info=False,
                )
                processed_successfully = False
                should_requeue = True

            except Exception as e:
                logging.error(
                    f'Error processing message (tag: {delivery_tag}): {e}',
                    exc_info=True,
                )
                processed_successfully = False
                should_requeue = requeue

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
                        f'Sending NACK (requeue={should_requeue}) for delivery_tag={delivery_tag} due to processing error.'
                    )
                    ch.basic_nack(delivery_tag=delivery_tag, requeue=should_requeue)

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
        self._exchange_name = exchange_name
        self._queue_name = queue
        super().__init__(broker)

    def _setup_topology(self, broker: Broker) -> str:
        broker.exchange_declare(self._exchange_name, 'fanout')
        queue_name = broker.queue_declare(self._queue_name)
        broker.queue_bind(self._exchange_name, queue_name)
        return queue_name

    def _get_requeue_policy(self) -> bool:
        return False


class NamedQueueConsumer(Consumer):
    def __init__(self, broker: RabbitMQBroker, queue_name: str):
        self._queue_name = queue_name
        super().__init__(broker)

    def _setup_topology(self, broker: Broker) -> str:
        broker.queue_declare(queue_name=self._queue_name, exclusive=False, durable=True)
        return self._queue_name

    def _get_requeue_policy(self) -> bool:
        return False


class ControlSignalConsumer(Consumer):
    def __init__(self, broker: Broker, exchange_name: str):
        self._exchange_name = exchange_name
        super().__init__(broker)

    def _setup_topology(self, broker: Broker) -> str:
        broker.exchange_declare(self._exchange_name, 'fanout')

        queue_name = broker.queue_declare(queue_name='', exclusive=True, durable=False)
        broker.queue_bind(self._exchange_name, queue_name)
        return queue_name

    def _get_requeue_policy(self) -> bool:
        return True
