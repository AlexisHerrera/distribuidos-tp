import logging
import signal

from src.messaging.broker import RabbitMQBroker
from src.messaging.connection import Connection
from src.messaging.consumer import NamedQueueConsumer
from src.messaging.protocol.message import Message, MessageType
from src.messaging.publisher import DirectPublisher
from src.model.movie import Movie
from src.utils.config import Config

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MovieFilter:
    def __init__(self, config: Config):
        self.config = config
        self.connection = self._setup_connection()
        self.running = True
        signal.signal(signal.SIGTERM, self._handle_termination)
        signal.signal(signal.SIGINT, self._handle_termination)

    def _setup_connection(self) -> Connection:
        try:
            broker = RabbitMQBroker(self.config.rabbit_host)
            # Output Queue
            output_queue_name = self.config.get_env_var(
                'OUTPUT_QUEUE', 'movies_single_country_queue'
            )
            publisher = DirectPublisher(broker, output_queue_name)
            # Input queue
            input_queue_name = self.config.get_env_var(
                'INPUT_QUEUE', 'movies_cleaned_queue'
            )
            consumer = NamedQueueConsumer(broker, input_queue_name)

            logger.info(
                f"Connection configured: Input='{input_queue_name}', Output='{output_queue_name}'"
            )
            return Connection(broker, publisher, consumer)
        except Exception as e:
            logger.error(f'Error configuring RabbitMQ: {e}', exc_info=True)
            raise

    def _handle_termination(self, signum, frame):
        logger.warning(f'SIGTERM signal received ({signum}). Closing connection...')
        self.running = False
        if self.connection:
            self.connection.close()
        logger.warning('Connection closed.')

    def process_message(self, message: Message):
        if not self.running:
            logger.warning('Message received during closing connection, ignoring.')
            return

        if message.message_type == MessageType.Movie:
            movies_list: list[Movie] = message.data
            if not isinstance(movies_list, list):
                logger.warning(f'Expected list for Movie data, got {type(movies_list)}')
                return
            logger.debug(f'Processing batch of {len(movies_list)} movies.')
            for movie in movies_list:
                passed_filter = False
                production_countries = list(movie.production_countries)
                if len(production_countries) == 1:
                    logger.info(
                        f"Movie ID={movie.id} ('{movie.title}') Passed Filter (1 country)."
                    )
                    passed_filter = True
                else:
                    logger.debug(
                        f"Movie ID={movie.id} ('{movie.title}') NOT passed the filter. Production countries: {movie.production_countries}"
                    )

                if passed_filter:
                    try:
                        output_message = Message(MessageType.Movie, [movie])
                        self.connection.send(output_message)
                        logger.debug(f'MovieID={movie.id} published in output queue.')
                    except Exception as e:
                        logger.error(
                            f'Error Publishing queue ID={movie.id}: {e}', exc_info=True
                        )

        elif message.message_type == MessageType.EOF:
            logger.info('EOF Received. Propagating...')
            try:
                eof_message = Message(MessageType.EOF, None)
                self.connection.send(eof_message)
            except Exception as e:
                logger.error(f'Failed at propagating EOF: {e}', exc_info=True)

        else:
            logger.warning(f'Unknown message: {message.message_type}')

    def run(self):
        if not self.connection:
            logger.error('Cannot run if connection is not initialized!')
            return

        logger.info('Start reading messages...')
        try:
            self.connection.recv(self.process_message)
            logger.info('Read messages stopped.')
        except KeyboardInterrupt:
            logger.warning('CTRL-C received, aborting...')
            self._handle_termination(signal.SIGINT, None)
        except Exception as e:
            logger.error(f'Failed while reading messages: {e}', exc_info=True)
            self._handle_termination(signal.SIGABRT, None)


if __name__ == '__main__':
    config = Config()

    try:
        filter_node = MovieFilter(config)
        filter_node.run()
    except Exception as e:
        logger.critical(f'Could not start filter node: {e}', exc_info=True)
        exit(1)
