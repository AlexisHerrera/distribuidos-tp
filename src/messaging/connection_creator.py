from src.messaging.broker import RabbitMQBroker
from src.messaging.connection import Connection
from src.messaging.consumer import BroadcastConsumer, NamedQueueConsumer
from src.messaging.publisher import BroadcastPublisher, DirectPublisher
from src.utils.config import Config


class ConnectionCreator:
    @staticmethod
    def create(config: Config) -> Connection:
        broker = RabbitMQBroker(config.rabbit_host)

        if config.output_queue:
            publisher = DirectPublisher(broker, config.output_queue)
        else:
            publisher = BroadcastPublisher(broker, config.publisher_exchange)

        if config.input_queue:
            consumer = NamedQueueConsumer(broker, config.input_queue)
        else:
            consumer = BroadcastConsumer(broker, config.consumer_exchange)

        return Connection(broker, publisher, consumer)
