# src/server/base_node.py
import logging
import os
import signal
import sys
import threading
import argparse
from abc import ABC, abstractmethod
from typing import Dict, Type, Any
from src.utils.config import Config
from src.messaging.broker import RabbitMQBroker
from src.messaging.consumer import Consumer
from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class BaseNode(ABC):
    def __init__(self, config: Config, node_type_arg: str):
        self.config = config
        self.node_type = node_type_arg
        self.logic: Any = None

        self.lock = threading.Lock()
        self._is_running = True
        self._shutdown_initiated = False

        self.broker: RabbitMQBroker | None = None
        self.consumer: Consumer | None = None
        self.input_queue: str | None = None

        try:
            self._load_logic()
            self._read_and_validate_config()
            self._setup_signal_handlers()
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

    def _read_and_validate_config(self):
        logger.debug('Reading common configuration...')
        try:
            self.rabbit_host = self.config.rabbit_host
            self.input_queue = self.config.input_queue

            if not self.rabbit_host:
                raise ValueError('RABBIT_HOST missing')
            if not self.input_queue:
                raise ValueError('INPUT_QUEUE missing')

            logger.debug(
                f'Common config OK: HOST={self.rabbit_host}, IN_Q={self.input_queue}'
            )
            self._check_specific_config()

        except Exception as e:
            logger.critical(f'Configuration error in BaseNode: {e}', exc_info=True)
            self._is_running = False
            raise

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

    def _connect_rabbitmq(self) -> bool:
        if not self.is_running():
            return False
        try:
            logger.info(f'Connecting to RabbitMQ at {self.rabbit_host}...')
            self.broker = RabbitMQBroker(self.rabbit_host)
            logger.info('Connected.')
            return True
        except Exception as e:
            logger.critical(f'Failed to connect RabbitMQ: {e}')
            self.shutdown(force=True)
            return False

    @abstractmethod
    def _setup_messaging_components(self):
        pass

    @abstractmethod
    def process_message(self, message: Message):
        pass

    def run(self):
        if not self.is_running():
            logger.critical('Node init failed.')
            return
        logger.info(f'Starting Node (Type: {self.node_type})...')
        if not self._connect_rabbitmq():
            self.shutdown(force=True)
            return

        try:
            self._setup_messaging_components()
            if not self.consumer:
                raise RuntimeError('Consumer not set up by subclass.')
            processing_callback = self.process_message
            if self.broker:
                in_q = self.input_queue
                logic_name = type(self.logic).__name__ if self.logic else 'N/A'
                logger.info(
                    f"Node '{logic_name}' running. Consuming from '{in_q}'. Waiting..."
                )

                self.consumer.consume(self.broker, processing_callback)

            logger.info('Consumer loop finished.')
        except KeyboardInterrupt:
            logger.warning('KeyboardInterrupt (CTRL-C)...')
        except Exception as e:
            logger.critical(f'Fatal error during run: {e}', exc_info=True)
        finally:
            self.shutdown(force=True)

    def shutdown(self, force=False):
        with self.lock:
            if not self._is_running and not force:
                return
            if self._shutdown_initiated and not force:
                return
            logic_name = type(self.logic).__name__ if self.logic else self.node_type
            logger.info(f"Shutdown requested for node '{logic_name}'. Force={force}")
            self._is_running = False
            self._shutdown_initiated = True
        # logger.debug('Attempting to cancel consumer...')
        # if self.consumer:
        #     try:
        #         self.consumer.cancel()
        #     except Exception as e:
        #         logger.debug(f'Ignoring error cancelling consumer: {e}')
        logger.debug('Closing broker connection...')
        if self.broker:
            try:
                self.broker.close()
                logger.info('RabbitMQ connection closed.')
            except Exception as e:
                logger.error(f'Error closing connection: {e}', exc_info=True)
        self.broker = None
        logger.info(f'Node {self.node_type} shutdown complete.')

    def is_running(self) -> bool:
        with self.lock:
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
        log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s [%(levelname)s] %(message)s',
            # force=True
        )
        logger = logging.getLogger(cls.__name__)
        try:
            args = cls._parse_args(logic_registry)
            config = Config()

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
