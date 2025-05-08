from src.messaging.broker import RabbitMQBroker
from src.messaging.connection import Connection, MultiPublisherConnection
from src.messaging.consumer import BroadcastConsumer, NamedQueueConsumer
from src.messaging.protocol.message import MessageType
from src.messaging.publisher import BroadcastPublisher, DirectPublisher
from src.utils.config import Config

EOF_EXCHANGE_PREFIX = 'eof_notify_'


class ConnectionCreator:
    @staticmethod
    def create(config: Config) -> Connection:
        broker = RabbitMQBroker(config.rabbit_host)

        consumers_cfg = config.consumers[0]
        queue_name = consumers_cfg['queue']
        if consumers_cfg['type'] == 'broadcast':
            consumer = BroadcastConsumer(
                broker,
                consumers_cfg['exchange'],
                queue_name,
            )
        else:
            consumer = NamedQueueConsumer(broker, queue_name)

        eof_publisher = None
        eof_consumer = None
        if config.replicas_enabled:
            eof_exchange = f'{EOF_EXCHANGE_PREFIX}{queue_name}'
            broker.exchange_declare(eof_exchange, 'fanout')
            # Creamos una cola exclusiva para recibir todos los EOF
            eof_queue = broker.queue_declare('', exclusive=True, durable=False)
            broker.queue_bind(eof_exchange, eof_queue)
            # Este consumer escuchará esa cola anónima
            eof_consumer = BroadcastConsumer(broker, eof_exchange, queue=None)
            eof_publisher = BroadcastPublisher(broker, eof_exchange)

        publishers_cfg = config.publishers[0]
        if publishers_cfg['type'] == 'broadcast':
            publisher = BroadcastPublisher(
                broker,
                publishers_cfg['queue'],
            )
        else:
            publisher = DirectPublisher(
                broker,
                publishers_cfg['queue'],
            )

        return Connection(broker, publisher, consumer, eof_publisher, eof_consumer)

    @staticmethod
    def create_multipublisher(config: Config) -> MultiPublisherConnection:
        broker_cons = RabbitMQBroker(config.rabbit_host)
        broker_pub = RabbitMQBroker(config.rabbit_host)

        consumers_config = config.consumers

        if len(consumers_config) > 0:
            if consumers_config[0]['type'] == 'broadcast':
                consumer = BroadcastConsumer(broker_cons, consumers_config[0]['queue'])
            else:
                consumer = NamedQueueConsumer(broker_cons, consumers_config[0]['queue'])

        publishers = {}

        for p in config.publishers:
            queue_name = p['queue']
            msg_type = MessageType(p['msg_type'])

            if p['type'] == 'broadcast':
                publisher = BroadcastPublisher(broker_pub, queue_name)
            else:
                publisher = DirectPublisher(broker_pub, queue_name)

            publishers[msg_type] = publisher

        return MultiPublisherConnection(broker_cons, broker_pub, publishers, consumer)
