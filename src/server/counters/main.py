import logging
import signal
import sys
import argparse

from src.messaging.broker import RabbitMQBroker
from src.messaging.consumer import NamedQueueConsumer
from src.messaging.protocol.message import Message, MessageType
from src.server.counters.base_counter_logic import BaseCounterLogic
from src.server.counters.country_budget_logic import CountryBudgetLogic
from src.utils.config import Config

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(name)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

AVAILABLE_COUNTER_LOGICS = {
    'country_budget': CountryBudgetLogic,
}


class GenericCounterNode:
    def __init__(self, config: Config, counter_type: str):
        self.config = config
        self.running = True
        self.counter_logic: BaseCounterLogic | None = None
        self.broker: RabbitMQBroker | None = None
        self.consumer: NamedQueueConsumer | None = None

        logger.info(f"Received counter type request: '{counter_type}'")

        logic_class = AVAILABLE_COUNTER_LOGICS.get(counter_type)
        if logic_class:
            logger.info(f'Loading counter logic: {logic_class.__name__}')
            try:
                self.counter_logic = logic_class()
                if hasattr(self.counter_logic, 'setup') and callable(
                    self.counter_logic.setup
                ):
                    self.counter_logic.setup()
            except Exception as e:
                logger.critical(
                    f'Failed to instantiate counter logic {logic_class.__name__}: {e}',
                    exc_info=True,
                )
                raise ValueError(
                    f'Could not instantiate counter logic {counter_type}'
                ) from e
        else:
            logger.critical(
                f"Unknown counter type: '{counter_type}'. Available: {list(AVAILABLE_COUNTER_LOGICS.keys())}"
            )
            raise ValueError(f'Invalid counter type specified: {counter_type}')

        try:
            self.rabbit_host = self.config.rabbit_host
            self.input_queue = self.config.get_env_var('INPUT_QUEUE')
            if not self.rabbit_host or not self.input_queue:
                raise ValueError(
                    'Missing essential configuration: RABBIT_HOST or INPUT_QUEUE'
                )
        except (ValueError, KeyError, AttributeError) as e:
            logger.critical(f'Error reading config: {e}', exc_info=True)
            self.running = False
            raise ValueError(f'Config error: {e}') from e

        self._setup_signal_handlers()
        logger.info(
            f'GenericCounterNode initialized (Type: {counter_type}). Reading from: {self.input_queue}'
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
        if not self.running:
            return False
        try:
            logger.info(f'Connecting to RabbitMQ at {self.rabbit_host}...')
            self.broker = RabbitMQBroker(self.rabbit_host)
            logger.info('Connected to RabbitMQ.')
            logger.info(f'Setting up consumer for queue: {self.input_queue}')
            self.consumer = NamedQueueConsumer(self.broker, self.input_queue)
            logger.info('RabbitMQ connection and consumer ready.')
            return True
        except Exception as e:
            logger.critical(f'Failed to connect/configure consumer: {e}', exc_info=True)
            self.running = False
            return False

    def process_message(self, message: Message):
        if not self.running:
            return

        try:
            if message.message_type == MessageType.EOF:
                logger.info('EOF Received by GenericCounterNode. Finalizing...')
                if self.counter_logic:
                    self.counter_logic.log_final_results()
                else:
                    logger.warning('No counter logic loaded to process results.')
                self.shutdown()
            elif self.counter_logic:
                self.counter_logic.process_message(message)
            else:
                logger.warning(
                    f'Received message type {message.message_type} but no logic loaded.'
                )

        except Exception as e:
            logger.error(f'Error during counter logic processing: {e}', exc_info=True)

    def shutdown(self):
        if not self.running:
            return
        logger.info(
            f'Shutting down counter node (type: {type(self.counter_logic).__name__})...'
        )
        self.running = False
        if self.broker:
            try:
                self.broker.close()
                logger.info('RabbitMQ connection closed.')
            except Exception as e:
                logger.error(f'Error closing connection: {e}', exc_info=True)
        logger.info('Counter node shutdown complete.')

    def run(self):
        if not self.running:
            logger.critical('Node init failed. Cannot run.')
            return
        if not self.counter_logic:
            logger.critical('Counter logic not loaded. Cannot run.')
            return
        if not self._connect_rabbitmq_and_setup_consumer():
            self.shutdown()
            return

        counter_type_name = type(self.counter_logic).__name__
        logger.info(f'Starting {counter_type_name} node...')
        logger.info(f'Reading messages from: {self.input_queue}')
        try:
            self.consumer.consume(self.broker, self.process_message)
            logger.info('Consumer loop finished gracefully.')
        except KeyboardInterrupt:
            logger.warning('CTRL-C detected. Initiating shutdown...')
        except Exception as e:
            logger.critical(f'Fatal error during consumption: {e}', exc_info=True)
        finally:
            self.shutdown()


def parse_args():
    parser = argparse.ArgumentParser(
        description='Generic Counter Node for Data Pipeline.'
    )
    parser.add_argument(
        'counter_type',
        metavar='COUNTER_TYPE',
        help='The type of counter logic to apply.',
        choices=AVAILABLE_COUNTER_LOGICS.keys(),
    )
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    counter_node_instance = None
    try:
        args = parse_args()
        config = Config()
        counter_node_instance = GenericCounterNode(config, args.counter_type)
        counter_node_instance.run()
        logger.info('GenericCounterNode run method finished.')
        sys.exit(0)
    except (ValueError, ImportError) as e:
        logger.critical(f'Failed to initialize node: {e}')
        sys.exit(1)
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        logger.critical(f'Unhandled exception in main: {e}', exc_info=True)
        if counter_node_instance:
            counter_node_instance.shutdown()
        sys.exit(1)
