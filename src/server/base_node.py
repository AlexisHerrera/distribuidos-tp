import logging
import signal
import sys
import argparse
from abc import ABC, abstractmethod

from src.messaging.publisher import DirectPublisher
from typing import Any, Dict, Type

from src.messaging.connection_creator import ConnectionCreator
from src.utils.config import Config
from src.messaging.consumer import NamedQueueConsumer
from src.messaging.protocol.message import Message, MessageType
from src.utils.log import initialize_log

logger = logging.getLogger(__name__)


class BaseNode(ABC):
    def __init__(self, config: Config, node_type_arg: str):
        self.config = config
        self.node_type = node_type_arg
        # Eg: single_country_logic
        self.logic: Any = None

        self._is_running = True
        self._shutdown_initiated = False
        self._eof_required = int(config.get_env_var('EOF_REQUIRED', '1'))
        self.connection = ConnectionCreator.create(config)

        try:
            self._load_logic()
            self._setup_signal_handlers()
            self._setup_messaging_components()
            logger.info(f"BaseNode for '{self.node_type}' initialized.")
        except Exception as e:
            logger.critical(
                f"Initialization failed for node '{self.node_type}': {e}", exc_info=True
            )
            self._is_running = False
            raise

    @abstractmethod
    def _get_logic_registry(self) -> Dict[str, Type]:
        pass

    def _load_logic(self):
        logic_registry = self._get_logic_registry()
        logic_class = logic_registry.get(self.node_type)
        if not logic_class:
            available = list(logic_registry.keys())
            raise ValueError(
                f"Invalid node type '{self.node_type}'. Available: {available}"
            )

        logger.info(f'Loading logic: {logic_class.__name__}')
        try:
            self.logic = logic_class()

            if hasattr(self.logic, 'setup') and callable(self.logic.setup):
                self.logic.setup()
        except Exception as e:
            logger.critical(
                f'Failed to instantiate/setup logic {logic_class.__name__}: {e}',
                exc_info=True,
            )
            raise ValueError(
                f'Could not instantiate/setup logic {self.node_type}'
            ) from e

    @abstractmethod
    def _check_specific_config(self):
        pass

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.shutdown()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Signal handlers configured.')

    @abstractmethod
    def _setup_messaging_components(self):
        logger.info(f'Starting Node (Type: {self.node_type})...')
        if not self._connect_rabbitmq():
            self.shutdown(force=True)
            raise ConnectionError('Could not connect to rabbit')
        logger.info(f'Setting up data consumer for queue: {self.config.input_queue}')
        self.consumer = NamedQueueConsumer(self.broker, self.config.input_queue)
        logger.info(f'Setting up data publisher for queue: {self.config.output_queue}')
        self.publisher = DirectPublisher(self.broker, self.config.output_queue)

    @abstractmethod
    def process_message(self, message: Message):
        pass

    def run(self):
        if not self.is_running():
            logger.critical('Node init failed.')
            return
        logger.info(f'Starting Node (Type: {self.node_type})...')

        try:
            logic_name = type(self.logic).__name__ if self.logic else 'N/A'
            logger.info(f"Node '{logic_name}' running. Consuming. Waiting...")

            self.connection.recv(self.process_message)

            logger.info('Consumer loop finished.')
        except KeyboardInterrupt:
            logger.warning('KeyboardInterrupt (CTRL-C)...')
        except Exception as e:
            logger.critical(f'Fatal error during run: {e}', exc_info=True)
        finally:
            self.shutdown(force=True)

    def shutdown(self, force=False):
        if not self._is_running and not force:
            return
        logic_name = type(self.logic).__name__ if self.logic else self.node_type
        logger.info(f"Shutdown requested for node '{logic_name}'. Force={force}")
        self._is_running = False
        logger.debug('Closing broker connection...')

        self.connection.close()
        logger.info(f'Node {self.node_type} shutdown complete.')

    def is_running(self) -> bool:
        return self._is_running

    @staticmethod
    def _parse_args(available_logics: dict) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description='Generic Pipeline Node.')
        parser.add_argument(
            'node_type',
            metavar='NODE_TYPE',
            help='The type of logic to apply.',
            choices=available_logics.keys(),
        )

        args = parser.parse_args()
        return args

    @classmethod
    def main(cls, logic_registry: Dict[str, Type]):
        node_instance = None
        config = Config()

        initialize_log(config.log_level)
        logger = logging.getLogger(cls.__name__)

        try:
            # Type of node
            args = cls._parse_args(logic_registry)

            node_instance = cls(config, args.node_type)
            node_instance.run()
            logger.info(f'{cls.__name__} run method finished.')
            sys.exit(0)
        except (ValueError, ImportError) as e:
            logger.critical(f'Failed to initialize {cls.__name__}: {e}')
            sys.exit(1)
        except SystemExit as e:
            sys.exit(e.code)
        except Exception as e:
            logger.critical(
                f'Unhandled exception in {cls.__name__}.main: {e}', exc_info=True
            )
        if node_instance and isinstance(node_instance, BaseNode):
            node_instance.shutdown(force=True)
        sys.exit(1)

    def _wrapped_callback(self, callback):
        def wrapper(message: Message):
            if message.message_type == MessageType.EOF:
                self._eof_required -= 1
                logger.info(f'EOF Received. {self._eof_required} left to process.')
                if self._eof_required <= 0:
                    try:
                        self.on_eof()
                    except AttributeError:
                        pass
                    eof_msg = Message(MessageType.EOF, None)
                    self.publisher.put(self.broker, eof_msg)
                    logger.info('EOF Published!')
                    self.shutdown()
                return
            callback(message)

        return wrapper

    @abstractmethod
    def on_eof(self):
        pass
