import argparse
import logging
import signal
import sys
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, Type

from src.messaging.connection_creator import ConnectionCreator
from src.messaging.protocol.message import Message, MessageType
from src.server.healthcheck import Healthcheck
from src.server.leader_election import LeaderElection
from src.utils.config import Config
from src.utils.in_flight_tracker import InFlightTracker
from src.utils.log import initialize_log
from src.utils.state_manager import StateManager

logger = logging.getLogger(__name__)
NODE_STATE_NAME = 'node_state.json'


class BaseNode(ABC):
    def __init__(
        self, config: Config, node_type_arg: str, has_result_persisted: bool = False
    ):
        self.config = config
        self.node_type = node_type_arg
        # Eg: single_country_logic
        self.logic: Any = None

        self._is_running = True
        self._shutdown_initiated = False
        # Lock to ensure thread-safe shutdown
        self._shutdown_lock = threading.Lock()
        self.connection = ConnectionCreator.create(config)
        self.leader = None
        self.in_flight_tracker = InFlightTracker()

        # sent results before eof
        self.has_results_persisted = has_result_persisted
        # Threads executor (should be instantiated on node)
        self._executor = None
        self.healthcheck = Healthcheck(config.healthcheck_port)

        self.node_state_manager: StateManager | None = None
        self.processed_message_ids: set[str] = set()
        try:
            peers = getattr(config, 'peers', [])
            port_str = getattr(config, 'port', None)
            self._load_logic()

            if isinstance(peers, list) and peers and port_str is not None:
                self.leader = LeaderElection(config, self)
            else:
                logger.info(
                    'Single-node configuration detected (peers list empty/missing or port missing). LeaderElection component will not be used.'
                )
                self.leader = None

            if self.has_results_persisted:
                self.node_state_manager = StateManager(NODE_STATE_NAME)
                self._load_node_state()

            self._setup_signal_handlers()
            logger.info(f"BaseNode for '{self.node_type}' initialized.")
        except Exception as e:
            logger.critical(
                f"Initialization failed for node '{self.node_type}': {e}", exc_info=True
            )
            self._is_running = False
            raise

    def _load_node_state(self):
        if not self.node_state_manager:
            return

        logger.info(
            f'Loading node state from {self.node_state_manager.state_file_path}...'
        )
        persisted_data = self.node_state_manager.load_state()

        if not persisted_data:
            logger.info('No previous state found or file is empty.')
            return

        processed_ids = persisted_data.get('processed_message_ids', [])
        self.processed_message_ids = set(processed_ids)
        logger.info(
            f'Restored {len(self.processed_message_ids)} processed message IDs.'
        )

        app_state = persisted_data.get('application_state')
        if app_state and hasattr(self.logic, 'load_application_state'):
            self.logic.load_application_state(app_state)
            logger.info('Application state restored successfully.')

    @abstractmethod
    def _get_logic_registry(self) -> Dict[str, Type]:
        pass

    def get_in_flight_count(self, user_id: uuid.UUID) -> int:
        return self.in_flight_tracker.get(user_id)

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

    def wait_for_executor(self):
        if getattr(self, '_executor', None):
            logger.info('Waiting for executor tasks to finish…')
            self._executor.shutdown(wait=True)
            logger.info('All executor tasks completed.')

    @abstractmethod
    def handle_message(self, message: Message):
        pass

    def send_final_results(self, user_id: uuid.UUID):
        if not self.has_results_persisted:
            return
        try:
            out_msg = self.logic.message_result(user_id)
            self.connection.thread_safe_send(out_msg)
            logger.info('Final results sent (result connection).')
        except Exception as e:
            logger.error(f'Error sending final counter results: {e}', exc_info=True)

    def process_message(self, message: Message):
        user_id = message.user_id

        if (
            self.has_results_persisted
            and str(message.message_id) in self.processed_message_ids
        ):
            logger.warning(
                f'Ignoring duplicate message with id {message.message_id} for user {user_id}'
            )
            return

        if message.message_type == MessageType.EOF:
            logger.info(f'RECEIVED EOF FROM QUEUE for user_id: {message.user_id}')
            if self.leader:
                # Multi-node
                logger.debug(
                    f'User {message.user_id}: Delegating EOF to LeaderElection.'
                )
                self.leader.handle_incoming_eof(message.user_id)
            else:
                # Single-Node
                self._finalize_single_node(message.user_id)
            return
        try:
            if self.leader:
                self.in_flight_tracker.increment(user_id)
            self.handle_message(message)
            if self.has_results_persisted:
                # chaos_test(0.01, f"Sink crashes processing: {message.message_id}")
                self.processed_message_ids.add(str(message.message_id))
                if hasattr(self.logic, 'get_application_state'):
                    app_state = self.logic.get_application_state()
                    state_to_persist = {
                        'processed_message_ids': list(self.processed_message_ids),
                        'application_state': app_state,
                    }
                    self.node_state_manager.save_state(state_to_persist)
                    # logger.info(f"Saving state: Msgs id: {len(list(self.processed_message_ids))}, app state: {len(app_state)}" )

        except Exception as e:
            logger.error(f'Error en handle_message: {e}', exc_info=True)
            raise
        finally:
            if self.leader:
                self.in_flight_tracker.decrement(user_id)

    def _finalize_single_node(self, user_id: uuid.UUID):
        logger.info(f'User {user_id}: Single-node waiting for executor tasks...')
        self.wait_for_executor()
        logger.info(f'User {user_id}: Single-node executor finished.')

        self.send_final_results(user_id)

        logger.info(f'User {user_id}: Single-node propagating EOF.')
        self.propagate_eof(user_id)
        logger.info(f'User {user_id}: Single-node finalization complete.')

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
                logger.info('Stopping leader election...')
                self.leader.stop()
                logger.info('Leader election stopped')
                self.healthcheck.stop()
            except Exception as e:
                logger.error(f'Stopping or closing error: {e}')

    def propagate_eof(self, user_id: uuid.UUID):
        try:
            logger.info('Propagating EOF to next stage...')
            eof_message = Message(user_id, MessageType.EOF, None, message_id=None)
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
