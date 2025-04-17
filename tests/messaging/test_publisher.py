import unittest
from unittest.mock import Mock
from src.messaging.broker import Broker
from src.messaging.protocol.message import Message
from src.messaging.publisher import BroadcastPublisher, DirectPublisher


class TestPublisher:
    def test_broadcast_publisher_init(self):
        broker = Mock(Broker)
        exchange_name = "test"

        _publisher = BroadcastPublisher(broker, exchange_name)

        broker.exchange_declare.assert_called_once_with(exchange_name, 'fanout')

    def test_broadcast_publisher_put(self):
        broker = Mock(Broker)
        message = Mock(Message)
        exchange_name = "test"

        msg = b'msg'

        message.to_bytes.return_value = msg

        publisher = BroadcastPublisher(broker, exchange_name)

        publisher.put(broker, message)

        broker.put.assert_called_once_with(exchange=exchange_name, body=msg)
        message.to_bytes.assert_called_once()

    def test_direct_publisher_init(self):
        broker = Mock(Broker)
        queue_name = "test"

        _publisher = DirectPublisher(broker, queue_name)

        broker.queue_declare.assert_called_once_with(queue_name)

    def test_direct_publisher_put(self):
        broker = Mock(Broker)
        message = Mock(Message)
        queue_name = "test"

        msg = b'msg'

        message.to_bytes.return_value = msg

        publisher = DirectPublisher(broker, queue_name)

        publisher.put(broker, message)

        broker.put.assert_called_once_with(routing_key=queue_name, body=msg)
        message.to_bytes.assert_called_once()


if __name__ == "__main__":
    unittest.main()
