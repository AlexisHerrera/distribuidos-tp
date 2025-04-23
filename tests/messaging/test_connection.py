import unittest
from unittest.mock import Mock

from src.messaging.broker import Broker
from src.messaging.connection import Connection, MultiPublisherConnection
from src.messaging.consumer import Consumer
from src.messaging.protocol.message import Message, MessageType
from src.messaging.publisher import Publisher


class TestConnection:
    def test_message_send(self):
        broker = Mock(Broker)
        publisher = Mock(Publisher)
        consumer = Mock(Consumer)

        connection = Connection(broker, publisher, consumer)

        message = Message(MessageType.Unknown, 'message')

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

        consumer.basic_consume.assert_called_once()

    def test_close(self):
        broker = Mock(Broker)
        publisher = Mock(Publisher)
        consumer = Mock(Consumer)

        connection = Connection(broker, publisher, consumer)

        connection.close()

        broker.close.assert_called_once()

    class TestMultiPublisherConnection:
        def test_send_to_movie(self):
            broker = Mock(Broker)
            consumer = Mock(Consumer)
            publisher = Mock(Publisher)
            msg_type = MessageType('Movie')
            message = Message(MessageType.Movie, [])

            publishers = {msg_type: publisher}

            multi = MultiPublisherConnection(broker, publishers, consumer)

            multi.send(message)

            publisher.put.assert_called_once_with(broker, message)

        def test_last_published_queue(self):
            broker = Mock(Broker)
            consumer = Mock(Consumer)
            publisher = Mock(Publisher)
            msg_type = MessageType('Movie')
            message = Message(MessageType.Movie, [])

            publishers = {msg_type: publisher}

            multi = MultiPublisherConnection(broker, publishers, consumer)

            multi.send(message)

            publisher.put.assert_called_with(broker, message)

            message = Message(MessageType.EOF, None)

            multi.send(message)

            publisher.put.assert_called_with(broker, message)

        def test_multiple_publishers(self):
            broker = Mock(Broker)
            consumer = Mock(Consumer)
            publisher_movie = Mock(Publisher)
            publisher_rating = Mock(Publisher)
            msg_type_movie = MessageType('Movie')
            msg_type_rating = MessageType('Rating')
            message_movie = Message(MessageType.Movie, [])
            message_rating = Message(MessageType.Rating, [])

            publishers = {
                msg_type_movie: publisher_movie,
                msg_type_rating: publisher_rating,
            }

            multi = MultiPublisherConnection(broker, publishers, consumer)

            multi.send(message_movie)

            publisher_movie.put.assert_called_once_with(broker, message_movie)
            publisher_rating.put.assert_not_called()

            multi.send(message_rating)

            publisher_rating.put.assert_called_once_with(broker, message_rating)
            publisher_movie.put.assert_called_once()


if __name__ == '__main__':
    unittest.main()
