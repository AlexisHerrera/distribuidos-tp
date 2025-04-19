import logging
import signal
import sys
from collections import defaultdict

from src.messaging.broker import RabbitMQBroker
from src.messaging.consumer import NamedQueueConsumer
from src.messaging.protocol.message import Message, MessageType
from src.model.movie import Movie
from src.utils.config import Config


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CountryBudgetCounter:
    def __init__(self, config: Config):
        logger.info('Initializing CountryBudgetCounter...')
        self.config = config
        self.is_running = True
        self.broker: RabbitMQBroker | None = None
        self.consumer: NamedQueueConsumer | None = None
        self.country_budgets = defaultdict(int)

        try:
            self.rabbit_host = self.config.rabbit_host
            self.input_queue = self.config.get_env_var(
                'INPUT_QUEUE', 'movies_single_country_queue'
            )
            if not self.rabbit_host or not self.input_queue:
                raise ValueError(
                    'Missing essential configuration: RABBIT_HOST or INPUT_QUEUE'
                )
        except (ValueError, KeyError, AttributeError) as e:
            logger.critical(
                f'Error reading configuration during init: {e}', exc_info=True
            )
            self.is_running = False
            raise ValueError(f'Configuration error: {e}') from e

        self._setup_signal_handlers()
        logger.info(
            f'CountryBudgetCounter initialized. Reading from: {self.input_queue}'
        )

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.shutdown()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Signal handlers configured.')

    def _connect_rabbitmq_and_setup_consumer(self):
        if not self.is_running:
            return False
        try:
            logger.info(f'Connecting to RabbitMQ at {self.rabbit_host}...')
            temp_broker_for_consumer = RabbitMQBroker(self.rabbit_host)
            self.broker = temp_broker_for_consumer

            logger.info(f'Setting up consumer for queue: {self.input_queue}')
            self.consumer = NamedQueueConsumer(
                temp_broker_for_consumer, self.input_queue
            )
            logger.info('RabbitMQ connection and consumer ready.')
            return True
        except Exception as e:
            logger.critical(
                f'Failed to connect/configure RabbitMQ consumer: {e}', exc_info=True
            )
            self.is_running = False
            return False

    def process_message(self, message: Message):
        if not self.is_running:
            logger.debug('Ignoring message received during shutdown.')
            return

        if message.message_type == MessageType.Movie:
            movie_list = message.data
            if not isinstance(movie_list, list) or len(movie_list) != 1:
                logger.warning(
                    f'Received Movie message, but data is not a list of 1 element: {type(movie_list)}, len={len(movie_list) if isinstance(movie_list, list) else "N/A"}'
                )
                return

            movie: Movie = movie_list[0]

            if not isinstance(movie, Movie):
                logger.warning(f'Data in list is not a Movie object: {type(movie)}')
                return
            if (
                not movie.production_countries
                or not isinstance(movie.production_countries, list)
                or len(movie.production_countries) != 1
            ):
                logger.warning(
                    f"Movie ID {movie.id} received, but has invalid 'production_countries': {movie.production_countries}. Skipping count."
                )
                return
            if not isinstance(movie.budget, (int, float)) or movie.budget < 0:
                logger.warning(
                    f"Movie ID {movie.id} received, but has invalid 'budget': {movie.budget}. Skipping count."
                )
                return

            country = movie.production_countries[0]
            budget = int(movie.budget)

            self.country_budgets[country] += budget
            logger.debug(
                f'Updated budget for {country}. New total: {self.country_budgets[country]}. Movie ID: {movie.id}, Budget: {budget}'
            )

        elif message.message_type == MessageType.EOF:
            logger.info('EOF Received. Finalizing counts and shutting down.')
            self.log_final_counts()
            self.shutdown()

        else:
            logger.warning(f'Received unknown message type: {message.message_type}')

    def log_final_counts(self):
        logger.info('--- Final Country Budget Counts ---')
        if not self.country_budgets:
            logger.info('No country budgets were counted.')
            return

        sorted_countries = sorted(
            self.country_budgets.items(), key=lambda item: item[1], reverse=True
        )

        logger.info('Top 5 Countries by Total Budget Invested (Single Production):')
        for i, (country, total_budget) in enumerate(sorted_countries[:5]):
            logger.info(f'  {i + 1}. {country}: {total_budget}')

        logger.info(f'Total countries counted: {len(sorted_countries)}')
        logger.info('------------------------------------')

    def shutdown(self):
        if not self.is_running:
            return
        logger.info('Shutting down CountryBudgetCounter...')
        self.is_running = False
        if self.broker:
            try:
                self.broker.close()
                logger.info('RabbitMQ broker connection closed.')
            except Exception as e:
                logger.error(
                    f'Error closing RabbitMQ broker connection: {e}', exc_info=True
                )
        logger.info('CountryBudgetCounter shutdown complete.')

    def run(self):
        if not self.is_running:
            logger.critical(
                'Counter not initialized correctly due to config error. Cannot run.'
            )
            return

        if not self._connect_rabbitmq_and_setup_consumer():
            self.shutdown()
            return

        logger.info(f'Starting consuming messages from queue: {self.input_queue}...')
        try:
            self.consumer.consume(self.broker, self.process_message)
            logger.info('Consumer loop finished.')

        except KeyboardInterrupt:
            logger.warning('CTRL-C detected. Initiating shutdown...')
            self.shutdown()
        except Exception as e:
            logger.critical(
                f'Fatal error during message consumption: {e}', exc_info=True
            )
            self.shutdown()
        finally:
            if self.is_running:
                self.shutdown()


if __name__ == '__main__':
    counter_instance = None
    try:
        config = Config()
        counter_instance = CountryBudgetCounter(config)
        counter_instance.run()
        logger.info('CountryBudgetCounter run method finished.')
        sys.exit(0)
    except ValueError as e:
        logger.critical(f'Failed to initialize Counter: {e}')
        sys.exit(1)
    except Exception as e:
        logger.critical(
            f'Unhandled exception in main execution block: {e}', exc_info=True
        )
        if counter_instance:
            counter_instance.shutdown()
        sys.exit(1)
