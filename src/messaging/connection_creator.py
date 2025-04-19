from src.messaging.broker import RabbitMQBroker
from src.messaging.connection import Connection
from src.messaging.consumer import BroadcastConsumer
from src.messaging.publisher import BroadcastPublisher
from src.utils.config import Config


class ConnectionCreator:
    @classmethod
    def create(cls, config: Config) -> Connection:
        broker = RabbitMQBroker(config.rabbit_host)
        publisher = BroadcastPublisher(broker, config.publisher_exchange)
        consumer = BroadcastConsumer(broker, config.consumer_exchange)

        return Connection(broker, publisher, consumer)
