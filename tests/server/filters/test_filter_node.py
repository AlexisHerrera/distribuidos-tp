import unittest
from unittest.mock import patch, MagicMock

from src.messaging.protocol.message import MessageType, Message
from src.model.movie import Movie
from src.server.filters.main import FilterNode
from src.server.filters.single_country_logic import SingleCountryLogic
from tests.mocks.mock_broker import MockBroker, MockChannel, MockMethod

# Descomenta si quieres ver logs de DEBUG durante el test
# logging.basicConfig(level=logging.DEBUG)
# logging.disable(logging.NOTSET)


class MockConfig:
    def __init__(self):
        self.rabbit_host = 'mock_host_set_in_init'

    def get_env_var(self, var_name, default=None):
        if var_name == 'INPUT_QUEUE':
            return 'test_input_q'
        if var_name == 'OUTPUT_QUEUE':
            return 'test_output_q'
        return default


class TestGenericFilterNode(unittest.TestCase):
    FILTER_TYPE_TO_TEST = 'solo_country'

    @patch('src.server.filters.main.RabbitMQBroker')
    def setUp(self, MockRabbitMQBroker):
        self.mock_broker_instance = MockBroker()
        MockRabbitMQBroker.return_value = self.mock_broker_instance
        self.mock_broker_instance.close = MagicMock(name='broker_instance_close')
        self.mock_filter_logic_instance = MagicMock(
            name=f'{self.FILTER_TYPE_TO_TEST}_logic_instance'
        )
        MockFilterLogicClass = MagicMock(name=f'{self.FILTER_TYPE_TO_TEST}_logic_class')
        MockFilterLogicClass.__name__ = SingleCountryLogic.__name__
        MockFilterLogicClass.return_value = self.mock_filter_logic_instance

        self.config = MockConfig()
        target_dict_path = 'src.server.filters.main.AVAILABLE_FILTER_LOGICS'
        mock_dict_values = {self.FILTER_TYPE_TO_TEST: MockFilterLogicClass}

        with patch.dict(target_dict_path, mock_dict_values, clear=True):
            self.filter_node = FilterNode(self.config, self.FILTER_TYPE_TO_TEST)

        self.assertIs(
            self.filter_node.filter_logic,
            self.mock_filter_logic_instance,
            'Patching dictionary failed - filter_node.filter_logic is not the expected mock instance!',
        )

        self.input_queue = self.config.get_env_var('INPUT_QUEUE')
        self.output_queue = self.config.get_env_var('OUTPUT_QUEUE')

        # Registrar consumidor (sin cambios)
        self.filter_node.connection.recv(self.filter_node.process_message)
        registered_callbacks = self.mock_broker_instance.consumers
        self.assertIn(self.input_queue, registered_callbacks, 'Consumer not registered')
        self.consumer_callback_wrapper = registered_callbacks.get(self.input_queue)

    def _create_movie_message(
        self, movie_id=1, title='Test Movie', countries=None
    ) -> bytes:
        # Usar datos por defecto simples, los detalles no importan para este test
        if countries is None:
            countries = ['Testland']
        movie = Movie(movie_id=movie_id, title=title, production_countries=countries)
        message = Message(MessageType.Movie, [movie])
        return message.to_bytes()

    def _create_eof_message(self) -> bytes:
        message = Message(MessageType.EOF, None)
        return message.to_bytes()

    def test_publishes_message_when_logic_returns_true(self):
        self.mock_filter_logic_instance.should_pass.return_value = True

        movie_bytes = self._create_movie_message()
        delivery_tag = 101
        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)

        self.consumer_callback_wrapper(mock_channel, mock_method, None, movie_bytes)
        self.mock_filter_logic_instance.should_pass.assert_called_once()
        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(
            len(published), 1, 'Node should publish when logic returns True'
        )
        published_msg = Message.from_bytes(published[0])
        self.assertEqual(published_msg.message_type, MessageType.Movie)
        self.assertIsInstance(published_msg.data, list)
        self.assertEqual(len(published_msg.data), 1)
        self.assertEqual(published_msg.data[0].id, 1)

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()

    def test_does_not_publish_when_logic_returns_false(self):
        self.mock_filter_logic_instance.should_pass.return_value = False

        movie_bytes = self._create_movie_message()
        delivery_tag = 102
        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)

        self.consumer_callback_wrapper(mock_channel, mock_method, None, movie_bytes)
        self.mock_filter_logic_instance.should_pass.assert_called_once()
        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(
            len(published), 0, 'Node should NOT publish when logic returns False'
        )
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()

    def test_eof_propagation_and_shutdown(self):
        eof_bytes = self._create_eof_message()
        delivery_tag = 103
        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)

        self.consumer_callback_wrapper(mock_channel, mock_method, None, eof_bytes)
        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(len(published), 1, 'Node should propagate EOF')
        published_msg = Message.from_bytes(published[0])
        self.assertEqual(published_msg.message_type, MessageType.EOF)

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()
        self.mock_broker_instance.close.assert_called_once()
        self.assertFalse(self.filter_node.running)

    def test_handles_filter_logic_exception(self):
        test_exception = ValueError('Filter logic failed!')
        self.mock_filter_logic_instance.should_pass.side_effect = test_exception

        movie_bytes = self._create_movie_message()
        delivery_tag = 104
        mock_channel = MockChannel()
        mock_method = MockMethod(delivery_tag=delivery_tag)

        self.consumer_callback_wrapper(mock_channel, mock_method, None, movie_bytes)
        self.mock_filter_logic_instance.should_pass.assert_called_once()
        published = self.mock_broker_instance.get_published_messages(self.output_queue)
        self.assertEqual(len(published), 0, 'Node should NOT publish when logic fails')

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=delivery_tag)
        mock_channel.basic_nack.assert_not_called()
