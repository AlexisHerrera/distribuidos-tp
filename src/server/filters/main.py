import argparse
import logging
import signal
import sys

from src.messaging.broker import RabbitMQBroker
from src.messaging.connection import Connection
from src.messaging.consumer import NamedQueueConsumer
from src.messaging.protocol.message import Message, MessageType
from src.messaging.publisher import DirectPublisher
from src.model.movie import Movie
from src.utils.config import Config
from src.server.filters.base_filter_logic import BaseFilterLogic
from src.server.filters.single_country_logic import SingleCountryLogic


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

AVAILABLE_FILTER_LOGICS = {
    'solo_country': SingleCountryLogic,
}


class FilterNode:
    def __init__(self, config: Config, filter_type: str):
        self.config = config
        self.running = True
        self.filter_logic: BaseFilterLogic | None = None

        logger.info(f"Received filter type request: '{filter_type}'")

        logic_class = AVAILABLE_FILTER_LOGICS.get(filter_type)
        if logic_class:
            logger.info(f'Loading filter logic: {logic_class.__name__}')
            try:
                self.filter_logic = logic_class()
                if hasattr(self.filter_logic, 'setup') and callable(
                    self.filter_logic.setup
                ):
                    self.filter_logic.setup()
            except Exception as e:
                logger.critical(
                    f'Failed to instantiate filter logic {logic_class.__name__}: {e}',
                    exc_info=True,
                )
                raise ValueError(
                    f'Could not instantiate filter logic {filter_type}'
                ) from e
        else:
            logger.critical(
                f"Unknown filter type specified: '{filter_type}'. Available: {list(AVAILABLE_FILTER_LOGICS.keys())}"
            )
            raise ValueError(f'Invalid filter type specified: {filter_type}')

        self.connection = self._setup_connection()
        self._setup_signal_handlers()

    def _setup_connection(self) -> Connection:
        try:
            broker = RabbitMQBroker(self.config.rabbit_host)
            output_queue_name = self.config.get_env_var('OUTPUT_QUEUE')
            input_queue_name = self.config.get_env_var('INPUT_QUEUE')
            if not output_queue_name or not input_queue_name:
                raise ValueError('Missing INPUT_QUEUE or OUTPUT_QUEUE')
            publisher = DirectPublisher(broker, output_queue_name)
            consumer = NamedQueueConsumer(broker, input_queue_name)
            logger.info(
                f"Connection configured: Input='{input_queue_name}', Output='{output_queue_name}'"
            )
            return Connection(broker, publisher, consumer)
        except Exception as e:
            logger.error(f'Error configuring RabbitMQ: {e}', exc_info=True)
            raise

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.shutdown()
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)
            logger.info('Signal handlers configured.')

    def shutdown(self):
        if not self.running:
            return
        logger.info(
            f'Shutting down filter node (type: {type(self.filter_logic).__name__})...'
        )
        self.running = False
        if self.connection:
            try:
                self.connection.close()
                logger.info('RabbitMQ connection closed.')
            except Exception as e:
                logger.error(f'Error closing connection: {e}', exc_info=True)
        logger.info('Filter node shutdown complete.')

    def process_message(self, message: Message):
        if not self.running:
            return

        if message.message_type == MessageType.Movie:
            movies_list: list[Movie] = message.data
            # print(
            #     f'DEBUG process_message: Received message data type: {type(movies_list)}'
            # )
            if not isinstance(movies_list, list):
                logger.warning(f'Expected list, got {type(movies_list)}')
                return

            for movie in movies_list:
                if not movie:
                    continue
                try:
                    passed_filter = self.filter_logic.should_pass(movie)
                except Exception as filter_exc:
                    logger.error(f'Filter logic error: {filter_exc}', exc_info=True)
                    passed_filter = False

                if passed_filter:
                    try:
                        output_message = Message(MessageType.Movie, [movie])
                        self.connection.send(output_message)
                        # logger.debug(f'MovieID={movie.id} published.')
                    except Exception as e:
                        logger.error(
                            f'Error Publishing ID={movie.id}: {e}', exc_info=True
                        )

        elif message.message_type == MessageType.EOF:
            logger.info('EOF Received. Propagating and shutting down...')
            try:
                eof_message = Message(MessageType.EOF, None)
                self.connection.send(eof_message)
                logger.info('EOF propagated.')
            except Exception as e:
                logger.error(f'Failed propagating EOF: {e}', exc_info=True)
            finally:
                self.shutdown()

        else:
            logger.warning(f'Unknown message type: {message.message_type}')

    def run(self):
        if not self.running or not self.connection or not self.filter_logic:
            logger.error('Node cannot run due to initialization errors.')
            return

        filter_type_name = type(self.filter_logic).__name__
        logger.info(f'Starting {filter_type_name} node...')
        logger.info(f'Reading messages from: {self.config.get_env_var("INPUT_QUEUE")}')
        try:
            self.connection.recv(self.process_message)
            logger.info('Consumer loop finished gracefully.')
        except KeyboardInterrupt:
            logger.warning('CTRL-C detected. Initiating shutdown...')
        except Exception as e:
            logger.critical(f'Fatal error during consumption: {e}', exc_info=True)
        finally:
            self.shutdown()


def parse_args():
    parser = argparse.ArgumentParser(description='Filter Node for Movie Data Pipeline.')
    parser.add_argument(
        'filter_type',
        metavar='FILTER_TYPE',
        help='The type of filter logic to apply.',
        choices=AVAILABLE_FILTER_LOGICS.keys(),
    )
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    filter_node_instance = None
    try:
        args = parse_args()
        config = Config()
        filter_node_instance = FilterNode(config, args.filter_type)
        filter_node_instance.run()
        logger.info('GenericFilterNode run method finished.')
        sys.exit(0)
    except (ValueError, ImportError) as e:
        logger.critical(f'Failed to initialize GenericFilterNode: {e}')
        sys.exit(1)
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        logger.critical(
            f'Unhandled exception in main execution block: {e}', exc_info=True
        )
        if filter_node_instance:
            filter_node_instance.shutdown()
        sys.exit(1)
