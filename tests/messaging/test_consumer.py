import unittest
from unittest.mock import ANY, Mock
from src.messaging.broker import Broker
from src.messaging.protocol.message import Message
from src.messaging.consumer import BroadcastConsumer


class TestConsumer:
    def test_broadcast_consumer_init(self):
        broker = Mock(Broker)
        exchange_name = 'test'
        queue_name = 'new_queue'

        broker.queue_declare.return_value = queue_name

        _consumer = BroadcastConsumer(broker, exchange_name)

        broker.exchange_declare.assert_called_once_with(exchange_name, 'fanout')
        broker.queue_declare.assert_called_once()
        broker.queue_bind.assert_called_once_with(exchange_name, queue_name)

    def test_broadcast_consumer_consume(self):
        broker = Mock(Broker)
        message = Mock(Message)
        exchange_name = 'test'
        queue_name = 'new_queue'

        broker.queue_declare.return_value = queue_name

        consumer = BroadcastConsumer(broker, exchange_name)

        consumer.consume(broker, message)

        broker.consume.assert_called_once_with(ANY, queue_name)


if __name__ == '__main__':
    unittest.main()
