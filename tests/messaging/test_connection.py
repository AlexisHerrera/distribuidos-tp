import unittest
from unittest.mock import Mock
from src.messaging.broker import Broker
from src.messaging.connection import Connection
from src.messaging.consumer import Consumer
from src.messaging.protocol.message import Message, MessageType
from src.messaging.publisher import Publisher


class TestConnection():
    def test_message_send(self):
        broker = Mock(Broker)
        publisher = Mock(Publisher)
        consumer = Mock(Consumer)

        connection = Connection(broker, publisher, consumer)

        message = Message(MessageType.Unknown, "message")

        connection.send(message)

        publisher.put.assert_called_once_with(broker, message)

    def test_message_recv(self):
        broker = Mock(Broker)
        publisher = Mock(Publisher)
        consumer = Mock(Consumer)

        connection = Connection(broker, publisher, consumer)

        def __callback():
            pass

        callback = Mock(__callback)

        connection.recv(callback=callback)

        consumer.consume.assert_called_once()


    def test_close(self):
        broker = Mock(Broker)
        publisher = Mock(Publisher)
        consumer = Mock(Consumer)

        connection = Connection(broker, publisher, consumer)

        connection.close()

        broker.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
