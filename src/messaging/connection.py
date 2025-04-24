import logging
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
        broker: Broker,
        publishers: dict[MessageType, Publisher],
        consumer: Consumer,
    ):
        self.__broker = broker
        self.__publishers = publishers
        self.__consumer = consumer
        self.__last_published_queue = None

    def send(self, message: Message):
        try:
            match message.message_type:
                case MessageType.EOF:
                    self.__publishers[self.__last_published_queue].put(
                        self.__broker, message
                    )
                case _ as msg_type:
                    self.__publishers[msg_type].put(self.__broker, message)
                    self.__last_published_queue = msg_type
        except KeyError:
            logger.error(f'Message type not in publishers {message.message_type}')

    def recv(self, callback: Callable[[Message], None]):
        self.__consumer.basic_consume(self.__broker, callback)
        self.__consumer.start_consuming(self.__broker, callback)

    def close(self):
        self.__broker.close()
