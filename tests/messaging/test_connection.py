import unittest
from unittest.mock import Mock, patch

from src.messaging.broker import Broker
from src.messaging.connection import Connection, MultiPublisherConnection, logger
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
            self.broker_mock = Mock(spec=Broker)
            self.consumer_mock = Mock(spec=Consumer)
            self.movie_publisher_mock = Mock(spec=Publisher)
            self.rating_publisher_mock = Mock(spec=Publisher)

            self.publishers = {
                MessageType.Movie: self.movie_publisher_mock,
                MessageType.Rating: self.rating_publisher_mock,
            }

            self.movie_publisher_mock.reset_mock()
            self.rating_publisher_mock.reset_mock()
            self.broker_mock.reset_mock()
            self.consumer_mock.reset_mock()

        def test_send_to_movie_with_threadsafe_attr(self):
            self.broker_mock.add_callback_threadsafe = Mock()

            multi_conn = MultiPublisherConnection(
                self.broker_mock, self.publishers, self.consumer_mock
            )
            movie_message = Message(1, MessageType.Movie, ['movie_data'])

            multi_conn.send(movie_message)

            self.broker_mock.add_callback_threadsafe.assert_called_once()
            args, _ = self.broker_mock.add_callback_threadsafe.call_args
            callback_fn = args[0]
            callback_fn()

            self.movie_publisher_mock.put.assert_called_once_with(
                self.broker_mock, movie_message
            )
            self.rating_publisher_mock.put.assert_not_called()

        def test_send_eof_with_threadsafe_attr(self):
            self.broker_mock.add_callback_threadsafe = Mock()

            multi_conn = MultiPublisherConnection(
                self.broker_mock, self.publishers, self.consumer_mock
            )
            eof_message = Message(1, MessageType.EOF, None)

            with patch.object(logger, 'info') as mock_log_info:
                multi_conn.send_eof(eof_message, target_queue_type=MessageType.Movie)

            self.broker_mock.add_callback_threadsafe.assert_called_once()
            args, _ = self.broker_mock.add_callback_threadsafe.call_args
            callback_fn = args[0]
            callback_fn()

            self.movie_publisher_mock.put.assert_called_once_with(
                self.broker_mock, eof_message
            )
            self.rating_publisher_mock.put.assert_not_called()
            mock_log_info.assert_any_call(
                f'Preparing to send EOF message for user_id {eof_message.user_id} to target queue type {MessageType.Movie}'
            )

        def test_multiple_publishers_routing_with_threadsafe_attr(self):
            captured_callbacks = []

            def mock_add_callback_threadsafe(callback_fn):
                captured_callbacks.append(callback_fn)

            self.broker_mock.add_callback_threadsafe = Mock(
                side_effect=mock_add_callback_threadsafe
            )

            multi_conn = MultiPublisherConnection(
                self.broker_mock, self.publishers, self.consumer_mock
            )

            user_id = 1
            movie_data = ['movie_data_payload_ts']
            rating_data = [4.5]

            message_movie = Message(user_id, MessageType.Movie, movie_data)
            message_rating = Message(user_id, MessageType.Rating, rating_data)

            multi_conn.send(message_movie)

            self.broker_mock.add_callback_threadsafe.assert_called_once()
            self.assertEqual(len(captured_callbacks), 1)

            movie_callback_fn = captured_callbacks.pop(0)
            movie_callback_fn()

            self.movie_publisher_mock.put.assert_called_once_with(
                self.broker_mock, message_movie
            )
            self.rating_publisher_mock.put.assert_not_called()

            multi_conn.send(message_rating)

            self.assertEqual(self.broker_mock.add_callback_threadsafe.call_count, 2)
            self.assertEqual(len(captured_callbacks), 1)

            rating_callback_fn = captured_callbacks.pop(0)
            rating_callback_fn()

            self.rating_publisher_mock.put.assert_called_once_with(
                self.broker_mock, message_rating
            )

            self.assertEqual(self.movie_publisher_mock.put.call_count, 1)


if __name__ == '__main__':
    unittest.main()
