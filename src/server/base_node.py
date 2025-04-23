import logging
import signal
import sys
import argparse
import threading
from abc import ABC, abstractmethod

from typing import Any, Dict, Type

from src.messaging.broker import RabbitMQBroker
from src.messaging.connection_creator import ConnectionCreator
from src.messaging.publisher import DirectPublisher
from src.server.leader_election import LeaderElection
from src.utils.config import Config
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
        # Lock to ensure thread-safe shutdown
        self._shutdown_lock = threading.Lock()
        self._eof_sent = False
        self.connection = ConnectionCreator.create(config)
        self.leader = LeaderElection(self.config)

        try:
            self._load_logic()
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

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.shutdown()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Signal handlers configured.')

    def _start_eof_monitor(self):
        if not self.leader.enabled:
            return
        # Espera EOF de líder o de peers
        self.leader.wait_for_eof()
        logger.info('EOF detected by monitor, closing connection…')
        # Cerrar conexión de forma segura
        self.shutdown()

    @abstractmethod
    def handle_message(self, message: Message):
        pass

    def process_message(self, message: Message):
        if message.message_type == MessageType.EOF:
            if self.leader.enabled:
                self.leader.on_local_eof()
            self.shutdown()
            return
        try:
            self.handle_message(message)
        except Exception as e:
            logger.error(f'Error en handle_message: {e}', exc_info=True)

    def run(self):
        if not self.is_running():
            logger.critical('Node init failed.')
            return
        logger.info(f'Starting Node (Type: {self.node_type})...')
        if self.leader.enabled:
            threading.Thread(target=self._start_eof_monitor, daemon=True).start()
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
            self.shutdown()

    def shutdown(self):
        with self._shutdown_lock:
            if self._shutdown_initiated:
                return
            self._shutdown_initiated = True

            logic_name = type(self.logic).__name__ if self.logic else self.node_type
            logger.info(f"Shutdown requested for node '{logic_name}'")
            self._is_running = False
            logger.debug('Coordinating EOF propagation and connection close...')

            # Leader sends EOF message to the next stage
            if not self.leader.enabled or (
                self.leader.enabled and self.leader.is_leader and not self._eof_sent
            ):
                try:
                    logger.info('Shutting down consumer first')
                    self.connection.stop_consuming()
                except Exception as e:
                    logger.warning(f'Stop consumming error: {e}')
                try:
                    logger.info('Propagating EOF to next stage...')
                    eof_broker = RabbitMQBroker(self.config.rabbit_host)
                    eof_publisher = DirectPublisher(
                        eof_broker, self.config.publishers[0]['queue']
                    )
                    eof_publisher.put(eof_broker, Message(MessageType.EOF, None))
                    self._eof_sent = True
                except Exception as e:
                    logger.error(f'Could not publish EOF: {e}')

            try:
                logging.info('Now closing broker connection')
                self.connection.close()
            except Exception as e:
                logger.debug(f'Ignoring connection.close error: {e}')

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
            node_instance.shutdown()
        sys.exit(1)
