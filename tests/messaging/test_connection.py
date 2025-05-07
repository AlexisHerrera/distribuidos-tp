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

        user_id = 1
        message = Message(user_id, MessageType.Unknown, 'message')

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

    class TestMultiPublisherConnection(unittest.TestCase):
        def setUp(self):
            self.broker_cons = Mock(spec=Broker)
            self.broker_pub = Mock(spec=Broker)
            self.consumer = Mock(spec=Consumer)
            self.pub_movie = Mock(spec=Publisher)
            self.publishers = {MessageType.Movie: self.pub_movie}
            self.conn = MultiPublisherConnection(
                self.broker_cons, self.broker_pub, self.publishers, self.consumer
            )

        def test_send_to_movie(self):
            user_id = 1
            msg = Message(user_id, MessageType.Movie, ['payload'])
            self.conn.send(msg)
            self.pub_movie.put.assert_called_once_with(self.broker_pub, msg)

        def test_send_eof(self):
            user_id = 1
            eof_msg = Message(user_id, MessageType.EOF, None)
            self.conn.send_eof(eof_msg, MessageType.Movie)
            self.pub_movie.put.assert_called_once_with(self.broker_pub, eof_msg)

        def test_multiple_publishers(self):
            pub_rating = Mock(spec=Publisher)
            pubs = {
                MessageType.Movie: self.pub_movie,
                MessageType.Rating: pub_rating,
            }
            conn = MultiPublisherConnection(
                self.broker_cons, self.broker_pub, pubs, self.consumer
            )

            msg_m = Message(1, MessageType.Movie, [])
            msg_r = Message(1, MessageType.Rating, [])

            conn.send(msg_m)
            self.pub_movie.put.assert_called_once_with(self.broker_pub, msg_m)
            pub_rating.put.assert_not_called()

            conn.send(msg_r)
            pub_rating.put.assert_called_once_with(self.broker_pub, msg_r)
            self.pub_movie.put.assert_called_once()


if __name__ == '__main__':
    unittest.main()
