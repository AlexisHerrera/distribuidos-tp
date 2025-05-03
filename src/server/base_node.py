import argparse
import logging
import signal
import sys
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, Type

from src.messaging.connection_creator import ConnectionCreator
from src.messaging.protocol.message import Message, MessageType
from src.server.leader_election import LeaderElection
from src.utils.config import Config
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
        self.connection = ConnectionCreator.create(config)
        self.leader = LeaderElection(self.config)
        # sent results before eof
        self.should_send_results_before_eof = False
        self._final_results_sent = False
        # executor
        self._executor = None
        try:
            self._load_logic()
            self._setup_signal_handlers()
            if self.leader.enabled:
                logger.info('Launching monitor thread')
                self._monitor_thread = threading.Thread(
                    target=self._start_eof_monitor, daemon=True
                )
                self._monitor_thread.start()
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

    def _wait_for_executor(self):
        if getattr(self, '_executor', None):
            logger.info('Waiting for executor tasks to finish…')
            self._executor.shutdown(wait=True)
            logger.info('All executor tasks completed.')

    def _start_eof_monitor(self):
        if not self.leader.enabled:
            return
        user_id = self.leader.wait_for_eof()
        logger.info('EOF detected by monitor')
        self._send_final_results(user_id)
        self._wait_for_executor()
        if self.leader.is_leader:
            logger.info('Leader waiting for DONE from followers…')
            self.leader.wait_for_done()
            logger.info('All followers DONE; propagating EOF downstream')
            self._propagate_eof(user_id)
        else:
            logger.info('Follower sending DONE to leader')
            self.leader.send_done()

    @abstractmethod
    def handle_message(self, message: Message):
        pass

    def _send_final_results(self, user_id: int):
        if not self.should_send_results_before_eof or self._final_results_sent:
            return
        try:
            out_msg = self.logic.message_result(user_id)
            self.connection.thread_safe_send(out_msg)
            logger.info('Final results sent (result connection).')
            self._final_results_sent = True
        except Exception as e:
            logger.error(f'Error sending final counter results: {e}', exc_info=True)

    def process_message(self, message: Message):
        if message.message_type == MessageType.EOF:
            logger.info('RECEIVED EOF FROM QUEUE')
            if not self.leader.enabled:
                # single node shutdown, there is no monitor
                self._wait_for_executor()
                self._send_final_results(message.user_id)
                self._propagate_eof(message.user_id)
            else:
                # Unlock monitor
                self.leader.on_local_eof(message.user_id)
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
            try:
                logging.info('Now closing broker connection...')
                self.connection.close()
                logging.info('Connection closed')
                if self.leader.enabled:
                    self.leader.stop()
                    logger.info('LeaderElection detenido')
                    self._monitor_thread.join()
            except Exception as e:
                logger.error(f'Stopping or closing error: {e}')

    def _propagate_eof(self, user_id: int):
        try:
            logger.info('Propagating EOF to next stage...')
            eof_message = Message(user_id, MessageType.EOF, None)
            self.connection.thread_safe_send(eof_message)

            logger.info(f'EOF SENT to queue {self.config.publishers[0]["queue"]}')
        except Exception as e:
            logger.error(f'Could not publish EOF: {e}')

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
