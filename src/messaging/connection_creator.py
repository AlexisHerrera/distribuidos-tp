from src.messaging.broker import RabbitMQBroker
from src.messaging.connection import Connection
from src.messaging.consumer import BroadcastConsumer
from src.messaging.publisher import BroadcastPublisher


class ConnectionCreator():
    @classmethod
    def create(cls, config) -> Connection:
        broker = RabbitMQBroker(config["host"])
        publisher = BroadcastPublisher(broker, config["publisher_exchange"])
        consumer = BroadcastConsumer(broker, config["consumer_exchange"])

        return Connection(broker, publisher, consumer)
