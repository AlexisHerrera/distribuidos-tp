from src.messaging.broker import RabbitMQBroker
from src.messaging.connection import Connection, MultiPublisherConnection
from src.messaging.consumer import BroadcastConsumer, NamedQueueConsumer
from src.messaging.protocol.message import MessageType
from src.messaging.publisher import BroadcastPublisher, DirectPublisher
from src.utils.config import Config


class ConnectionCreator:
    @staticmethod
    def create(config: Config) -> Connection:
        broker = RabbitMQBroker(config.rabbit_host)

        consumers_config = config.consumers

        if len(consumers_config) > 0:
            if consumers_config[0]['type'] == 'broadcast':
                consumer = BroadcastConsumer(
                    broker,
                    consumers_config[0]['exchange'],
                    consumers_config[0]['queue'],
                )
            else:
                consumer = NamedQueueConsumer(broker, consumers_config[0]['queue'])

        publiser_config = config.publishers

        if len(publiser_config) > 0:
            if publiser_config[0]['type'] == 'broadcast':
                publisher = BroadcastPublisher(broker, publiser_config[0]['queue'])
            else:
                publisher = DirectPublisher(broker, publiser_config[0]['queue'])

        return Connection(broker, publisher, consumer)

    @staticmethod
    def create_multipublisher(config: Config) -> MultiPublisherConnection:
        broker = RabbitMQBroker(config.rabbit_host)

        consumers_config = config.consumers

        if len(consumers_config) > 0:
            if consumers_config[0]['type'] == 'broadcast':
                consumer = BroadcastConsumer(broker, consumers_config[0]['queue'])
            else:
                consumer = NamedQueueConsumer(broker, consumers_config[0]['queue'])

        publishers = {}

        for p in config.publishers:
            queue_name = p['queue']
            msg_type = MessageType(p['msg_type'])

            if p['type'] == 'broadcast':
                publisher = BroadcastPublisher(broker, queue_name)
            else:
                publisher = DirectPublisher(broker, queue_name)

            publishers[msg_type] = publisher

        return MultiPublisherConnection(broker, publishers, consumer)
