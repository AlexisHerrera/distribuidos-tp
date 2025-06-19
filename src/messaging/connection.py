import logging
import threading
from typing import Callable

from src.messaging.broker import Broker
from src.messaging.consumer import Consumer
from src.messaging.protocol.message import Message, MessageType
from src.messaging.publisher import Publisher

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, broker: Broker, publisher: Publisher, consumer: Consumer):
        self.__broker = broker
        self.__publisher = publisher
        self.__consumer = consumer
        self.consumer_tag = None

    def send(self, message: Message):
        self.__publisher.put(self.__broker, message)

    def thread_safe_send(self, message: Message):
        def _publish():
            self.__publisher.put(self.__broker, message)

        if hasattr(self.__broker, 'add_callback_threadsafe'):
            self.__broker.add_callback_threadsafe(_publish)
        else:
            _publish()

    def recv(self, callback: Callable[[Message], None]):
        self.consumer_tag = self.__consumer.basic_consume(self.__broker, callback)
        self.__consumer.start_consuming(self.__broker, callback)

    def close(self):
        try:
            logger.info('Closing broker channel and connection…')
            self.__broker.close()
        except Exception as e:
            logger.error(f'Error closing broker: {e}')


class MultiPublisherConnection:
    def __init__(
        self,
        broker_cons: Broker,
        broker_pub: Broker,
        publishers: dict[MessageType, Publisher],
        consumer: Consumer,
    ):
        self.__broker_cons = broker_cons
        self.__broker_pub = broker_pub
        self.__publishers = publishers
        self.__consumer = consumer
        self._pub_lock = threading.Lock()

    def _get_publisher(self, message_type: MessageType) -> Publisher | None:
        return self.__publishers.get(message_type)

    def _perform_publish(self, publisher: Publisher, message: Message):
        try:
            publisher.put(self.__broker_pub, message)
        except Exception as e:
            logger.error(
                f'Exception during publisher.put for message type {message.message_type} (targetting publisher for {message.message_type if message.message_type != MessageType.EOF else "EOF"}): {e}',
                exc_info=True,
            )

    def send(self, message: Message):
        if message.message_type == MessageType.EOF:
            logger.error(
                "Attempted to send EOF message using 'send' method. Please use 'send_eof' instead."
            )
            return

        publisher = self._get_publisher(message.message_type)
        if not publisher:
            logger.error(
                f'Message type {message.message_type} not in publishers for send'
            )
            return

        with self._pub_lock:
            self._perform_publish(publisher, message)

    def send_eof(self, eof_message: Message, target_queue_type: MessageType):
        if eof_message.message_type != MessageType.EOF:
            logger.error(
                f'[{eof_message.user_id}] send_eof method called with a message that is not of type EOF. Message was not sent.'
            )
            return

        publisher = self._get_publisher(target_queue_type)
        if not publisher:
            logger.error(
                f'[{eof_message.user_id}] Target queue type {target_queue_type} for EOF not in publishers'
            )
            return

        logger.info(
            f'[{eof_message.user_id}] Preparing to send EOF message to target queue type {target_queue_type}'
        )

        with self._pub_lock:
            self._perform_publish(publisher, eof_message)

    def recv(self, callback: Callable[[Message], None]):
        self.__consumer.basic_consume(self.__broker_cons, callback)
        self.__consumer.start_consuming(self.__broker_cons, callback)

    def close(self):
        self.__broker_pub.close()
        self.__broker_cons.close()
