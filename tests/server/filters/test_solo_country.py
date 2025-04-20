import unittest
from unittest.mock import patch

from src.messaging.protocol.message import Message, MessageType
from src.model.movie import Movie
from src.server.filters.single_country import MovieFilter
from tests.mocks.mock_broker import MockBroker, MockChannel, MockMethod

# logging.disable(logging.CRITICAL)


class MockConfig:
    def __init__(self):
        self.rabbit_host = 'mock_host_set_in_init'

    def get_env_var(self, var_name, default=None):
        if var_name == 'INPUT_QUEUE':
            return 'test_input_q'
        if var_name == 'OUTPUT_QUEUE':
            return 'test_output_q'
        return default


class TestMovieFilter(unittest.TestCase):
    @patch('src.server.filters.single_country.RabbitMQBroker')
    def setUp(self, broker):
        self.mock_broker_instance = MockBroker()
        broker.return_value = self.mock_broker_instance

        self.config = MockConfig()
        self.movie_filter = MovieFilter(self.config)

        self.input_queue = self.config.get_env_var('INPUT_QUEUE')
        self.output_queue = self.config.get_env_var('OUTPUT_QUEUE')

        self.movie_filter.connection.recv(self.movie_filter.process_message)
        registered_callbacks = self.mock_broker_instance.consumers

        self.assertIn(
            self.input_queue,
            registered_callbacks,
            f"No consumer registered for queue '{self.input_queue}' in mock after calling connection.recv",
        )
        consumer_callback_wrapper = registered_callbacks.get(self.input_queue)
        self.consumer_callback_wrapper = consumer_callback_wrapper

    def _create_movie_message(self, movie_id, title, countries) -> bytes:
        movie = Movie(movie_id=movie_id, title=title, production_countries=countries)
        message = Message(MessageType.Movie, [movie])
        return message.to_bytes()

    def _create_eof_message(self) -> bytes:
        message = Message(MessageType.EOF, None)
        return message.to_bytes()

    def test_process_message_pass_single_country(self):
        movie_bytes = self._create_movie_message(1, 'Relatos Salvajes', ['Argentina'])
        delivery_tag = 101

        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)
        self.consumer_callback_wrapper(mock_channel, mock_method, None, movie_bytes)

        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(len(published), 1, 'Should publish 1 message')
        published_msg = Message.from_bytes(published[0])
        self.assertEqual(published_msg.message_type, MessageType.Movie)
        self.assertEqual(published_msg.data[0].id, 1)
        self.assertEqual(published_msg.data[0].production_countries, ['Argentina'])

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()

    def test_process_message_fail_multiple_countries(self):
        movie_bytes = self._create_movie_message(
            2, 'Lost in Translation', ['USA', 'Japan']
        )
        delivery_tag = 102

        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)

        self.consumer_callback_wrapper(mock_channel, mock_method, None, movie_bytes)

        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(
            len(published), 0, 'Should not publish any message (multiple countries)'
        )

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()

    def test_process_message_fail_no_countries(self):
        movie_bytes = self._create_movie_message(3, 'No countries', None)
        delivery_tag = 103

        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)

        self.consumer_callback_wrapper(mock_channel, mock_method, None, movie_bytes)

        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(
            len(published), 0, 'Should not publish any message (None countries)'
        )

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()

    def test_process_message_fail_empty_countries(self):
        movie_bytes = self._create_movie_message(4, 'Empty countries', [])
        delivery_tag = 104

        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)

        self.consumer_callback_wrapper(mock_channel, mock_method, None, movie_bytes)

        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(
            len(published), 0, 'Should not publish any message (empty countries list)'
        )

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()

    def test_process_message_eof_propagates(self):
        eof_bytes = self._create_eof_message()
        delivery_tag = 105

        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)

        self.consumer_callback_wrapper(mock_channel, mock_method, None, eof_bytes)

        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(len(published), 1, 'Should publish EOF (propagation)')
        published_msg = Message.from_bytes(published[0])
        self.assertEqual(
            published_msg.message_type,
            MessageType.EOF,
            'Published message should be EOF',
        )

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()
