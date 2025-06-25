import logging
import os
import queue
import signal
import socket
import sys
import threading
import uuid
from queue import Queue
from typing import Callable

from src.common.protocol.batch import Batch, BatchType, batch_to_list_objects
from src.common.socket_communication import (
    create_server_socket,
    receive_message,
    send_message,
)
from src.messaging.connection_creator import ConnectionCreator
from src.messaging.protocol.message import Message, MessageType
from src.server.cleaner.CleanerStateMachine import CleanerStateMachine
from src.server.healthcheck import Healthcheck
# from src.server.leader_election import chaos_test

# from src.server.leader_election import chaos_test
from src.utils.config import Config
from src.utils.log import initialize_log
from src.utils.state_manager import StateManager
from src.utils.wal_manager import WALManager

logger = logging.getLogger(__name__)

CLEANER_STATE_DIR = '/app/state'
STATE_FILE = 'state.json'
PENDING_RESULTS_WAL_DIR = os.path.join(CLEANER_STATE_DIR, 'pending_results_wal')


class Cleaner:
    def __init__(self, config: Config):
        logger.info('Initializing Cleaner...')
        self.config = config
        self.is_running = True
        self.healthcheck: Healthcheck = Healthcheck(config.healthcheck_port)
        self.connection = ConnectionCreator.create_multipublisher(config)
        self.server_socket = None
        self.expected_query_count = 5

        # State management
        self.state_manager = StateManager(STATE_FILE)
        # Wal manager for results
        self.pending_results_wal_manager = WALManager(PENDING_RESULTS_WAL_DIR)

        # clients = { "user_id" : { 'socket': client_socket, 'queries_received': number, 'lock': threading.Lock()}}
        self.clients = {}
        self.clients_lock = threading.Lock()
        self._load_clients()
        # Sender Thread
        self.send_queue: Queue[tuple[uuid.UUID, bytes, str]] = queue.Queue()
        self.sender_thread = threading.Thread(
            target=self._send_results_to_client_loop, name='SenderThread', daemon=True
        )
        self.results_processing_thread = threading.Thread(
            target=self._receive_results_loop, name='ResultsQueueThread', daemon=True
        )

        try:
            self.port = int(os.getenv('SERVER_PORT', '12345'))
            self.backlog = int(os.getenv('LISTENING_BACKLOG', '3'))

        except (ValueError, KeyError, AttributeError) as e:
            logger.critical(
                f'Error reading configuration during init: {e}', exc_info=True
            )
            self.is_running = False
            raise ValueError(f'Configuration error: {e}') from e

        logger.info('Cleaner initialized.')

    def _handshake(self, client_socket, address) -> uuid.UUID | None:
        def update_socket(state):
            state['socket'] = client_socket

        # Handshake
        raw_handshake = receive_message(client_socket)
        handshake_msg = Message.from_bytes(raw_handshake)

        if handshake_msg.message_type != MessageType.HANDSHAKE_SESSION:
            logger.error(
                f'Connection from {address} did not start with a handshake. Closing.'
            )
            return None

        client_provided_id = handshake_msg.user_id
        is_new_client = client_provided_id == uuid.UUID(int=0)

        if is_new_client:
            user_id = self.generate_user_id()
            logger.info(
                f'New client from {address}. Assigned new session ID: {user_id}'
            )

            self._update_client_state(user_id, update_socket)
        else:
            user_id = client_provided_id
            logger.info(
                f'Client reconnected from {address}. Resuming session ID: {user_id}'
            )
            self._update_client_state(user_id, update_socket)

        # Confirm handshake
        response_msg = Message(user_id, MessageType.HANDSHAKE_SESSION)
        send_message(client_socket, response_msg.to_bytes())
        return user_id

    def _handle_single_client(self, client_socket, address_tuple):
        """
        Starts handshake, and if everything is ok, calls process data.
        """
        address = f'{address_tuple[0]}:{address_tuple[1]}'
        user_id = None

        try:
            # handshake
            user_id = self._handshake(client_socket, address)
            if user_id is None:
                return
            self._process_client_data(user_id, client_socket)
        except (
            ConnectionError,
            ConnectionResetError,
            ConnectionAbortedError,
            socket.error,
        ) as e:
            logger.warning(
                f'[{user_id}] Connection error with client {address}: {e}. Disconnecting.'
            )
        except Exception as e:
            logger.error(
                f'[{user_id}] Unhandled error in client handler for {address}: {e}',
                exc_info=True,
            )
        finally:
            logger.debug(f'[{user_id}] Client handler thread for {address} is ending.')

    def _update_client_state(
        self, user_id: uuid.UUID, update_func: Callable[[dict], None]
    ):
        """Gives a function to modify client state"""
        with self.clients_lock:
            client_state = self.clients.setdefault(
                user_id,
                {
                    'user_id': user_id,
                    'stage': CleanerStateMachine.STAGES[0],
                    'queries_received': 0,
                    'socket': None,
                    'lock': threading.Lock(),
                },
            )
            update_func(client_state)
            self._save_all_clients_state()

    def _save_all_clients_state(self):
        """Save all whole cleaner state. Should be called with a lock at clients"""
        state_to_persist = {}
        for user_id, data in self.clients.items():
            state_to_persist[str(user_id)] = {
                'stage': data['stage'],
                'queries_received': data['queries_received'],
            }
        self.state_manager.save_state(state_to_persist)

    def _load_clients(self):
        logger.info('Loading client states from disk...')
        persisted_states = self.state_manager.load_state()

        with self.clients_lock:
            for user_id_str, data in persisted_states.items():
                user_id = uuid.UUID(user_id_str)
                self.clients[user_id] = {
                    'user_id': user_id,
                    'stage': data.get('stage', CleanerStateMachine.STAGES[0]),
                    'queries_received': data.get('queries_received', 0),
                    'socket': None,
                    'lock': threading.Lock(),
                }

        logger.info(f'Restored {len(self.clients)} client states into memory.')

    def _update_stage(self, user_id: uuid.UUID, stage):
        def stage_update_function(state):
            state['stage'] = stage

        self._update_client_state(user_id, stage_update_function)

    def _cleanup_pending_results_wal(self, user_id: uuid.UUID):
        """Deletes al results from a user, it is only executed when client has received all"""
        logger.info(f'[{user_id}] Cleaning up pending result WAL files.')
        wal_dir = self.pending_results_wal_manager.wal_dir
        user_id_prefix = str(user_id)

        try:
            for filename in os.listdir(wal_dir):
                if filename.startswith(user_id_prefix) and filename.endswith('.wal'):
                    filepath_to_remove = os.path.join(wal_dir, filename)
                    self.pending_results_wal_manager.remove_entry(filepath_to_remove)
        except FileNotFoundError:
            logger.warning(
                f'WAL directory {wal_dir} not found during cleanup for user {user_id}. Nothing to do.'
            )
        except Exception as e:
            logger.error(
                f'[{user_id}] Error during pending results WAL cleanup: {e}',
                exc_info=True,
            )

    def _cleanup_client(self, user_id: uuid.UUID):
        logger.info(f'[{user_id}] Cleaning up all resources and persistent state.')
        self._cleanup_pending_results_wal(user_id)

        with self.clients_lock:
            client_info = self.clients.pop(user_id, None)
            if client_info:
                self._save_all_clients_state()
                sock = client_info.get('socket')
                if sock:
                    try:
                        sock.close()
                    except (socket.error, OSError):
                        pass

    def _process_client_data(self, user_id: uuid.UUID, client_socket: socket.socket):
        # Get client lock
        with self.clients_lock:
            client_lock = self.clients[user_id]['lock']

        with self.clients_lock:
            client_state = self.clients.get(user_id)
            if not client_state or not client_state.get('socket'):
                logger.warning(
                    f'[{user_id}] Session terminated or socket closed externally.'
                )
                return

        state_machine = CleanerStateMachine(client_state['stage'])
        while self.is_running and not state_machine.is_finished():
            # Get client data. If fails, caller will catch the exception
            window_message_bytes = receive_message(client_socket)
            window_message = Message.from_bytes(window_message_bytes)
            if window_message.message_type != MessageType.WINDOW:
                raise ValueError(f'Expected WINDOW message, received: {window_message}')
            batches_to_read = int(window_message.data)

            for _i in range(batches_to_read):
                # if i == 5:
                #     chaos_test(0.01, 'Crash in middle of batch read.'
                #                      'Should go back to last ACK')
                # it is guaranteed that client will
                # not send data message after EOF, until receives from client an ACK
                raw_batch_bytes = receive_message(client_socket)
                # This updates client_state['stage'] to make this loop finish
                self._process_and_send_batch(user_id, raw_batch_bytes, state_machine)

            ack_message = Message(user_id, MessageType.ACK, None)
            with client_lock:
                send_message(client_socket, ack_message.to_bytes())

    def _process_and_send_batch(
        self,
        user_id: uuid.UUID,
        raw_batch_bytes: bytes,
        state_machine: CleanerStateMachine,
    ):
        # chaos_test(0.001, 'Crash 0.1% before reading from TCP')
        batch = Batch.from_bytes(raw_batch_bytes)
        if not batch:
            raise ValueError(f'[{user_id}] Failed to decode batch during processing.')

        config_current_stage = state_machine.get_current_config()
        expected_batch_type = config_current_stage['batch_type']
        if batch.type == BatchType.EOF:
            # Depending on current stage, it will send an EOF to the queue
            eof_message_type = config_current_stage['message_type']
            self._publish_eof(user_id, eof_message_type)
            state_machine.advance()
            self._update_stage(user_id, state_machine.stage)
            return

        # If it goes here, then will queue data
        message_type_map = {
            BatchType.MOVIES: MessageType.Movie,
            BatchType.CREDITS: MessageType.Cast,
            BatchType.RATINGS: MessageType.Rating,
        }
        associated_message_type = message_type_map.get(batch.type)
        if not associated_message_type:
            logger.warning(f'No MessageType mapping for BatchType: {batch.type}')
            raise ValueError(f'Invalid type: {batch.type}')

        # If state is advanced, it will go back
        if expected_batch_type != batch.type:
            stage_to_update = CleanerStateMachine.BATCH_TO_STAGE[batch.type]
            logger.warning(
                f'Unexpected batch. Expecting {expected_batch_type}, got {batch.type}. Going to stage: {stage_to_update}'
            )
            self._update_stage(user_id, stage_to_update)

        object_list = batch_to_list_objects(batch)
        if object_list:
            output_message = Message(user_id, associated_message_type, object_list)
            self.connection.send(output_message)
            logger.debug(
                f'[{user_id}] Published batch of {len(object_list)} objects of type {associated_message_type.name}.'
            )

    def _publish_eof(self, user_id: uuid.UUID, stream_message_type: MessageType):
        try:
            logger.info(
                f'[{user_id}] Publishing stream EOF for {stream_message_type.name} to message queue.'
            )
            eof_message_for_stream = Message(user_id, MessageType.EOF, None)
            self.connection.send_eof(
                eof_message_for_stream, target_queue_type=stream_message_type
            )
        except Exception as e:
            logger.error(
                f'[{user_id}] Failed to publish stream EOF for {stream_message_type.name}: {e}',
                exc_info=True,
            )

    def _receive_results_loop(self):
        logger.info(
            'Result processing thread started. Waiting for messages from queue...'
        )
        try:
            self.connection.recv(self._process_message_from_queue)
        except Exception as e:
            if self.is_running:
                logger.critical(
                    f'Fatal error in result processing thread: {e}', exc_info=True
                )
        finally:
            logger.info('Result processing thread finished.')

    def _process_message_from_queue(self, message: Message):
        user_id = message.user_id

        with self.clients_lock:
            client_info = self.clients.get(user_id)

        if not client_info:
            if message.message_type != MessageType.EOF:
                logger.warning(
                    f'[N/A User] Received message for unknown or disconnected user_id: '
                    f'{user_id}, type: {message.message_type.name}. Cannot deliver.'
                )
            else:
                logger.debug(
                    f'[N/A User] Received EOF for user_id: {user_id} not active or already cleaned up.'
                )
            return

        if message.message_type == MessageType.EOF:
            logger.debug(
                f'[{user_id}] Received EOF from processing queue. Not a query result.'
            )
            return

        code = {
            MessageType.Movie: 'Q1 (Movie)',
            MessageType.MovieBudgetCounter: 'Q2 (MovieBudgetCounter)',
            MessageType.MovieRatingAvg: 'Q3 (MovieRatingAvg)',
            MessageType.ActorCount: 'Q4 (ActorCount)',
            MessageType.MovieAvgBudget: 'Q5 (MovieAvgBudget)',
        }.get(message.message_type, f'Undefined ({message.message_type.name})')

        logger.info(f'[{user_id}] Received query result: {code}')

        payload = message.to_bytes()
        try:
            wal_filepath = self.pending_results_wal_manager.write_entry(
                user_id, message.message_type.name, payload
            )
        except Exception as e:
            logger.critical(
                f'[{user_id}] Failed to persist result to WAL. Message might be lost on restart. Error: {e}',
                exc_info=True,
            )
            wal_filepath = None

        if wal_filepath:
            self.send_queue.put((user_id, payload, wal_filepath))
            logger.info(
                f'[{user_id}] Enqueued {code} for sending and persisted to WAL.'
            )
        else:
            # WAL Failed. Ignored because it will retry
            pass

    def _send_results_to_client_loop(self):
        while self.is_running or not self.send_queue.empty():
            try:
                user_id, payload, wal_filepath = self.send_queue.get(timeout=1)
            except queue.Empty:
                continue

            with self.clients_lock:
                client_info = self.clients.get(user_id)

            if not client_info or not client_info.get('socket'):
                logger.warning(
                    f'[{user_id}] Client disconnected. Result from {wal_filepath} will be re-queued on next restart.'
                )
                self.send_queue.task_done()
                continue

            client_socket = client_info['socket']
            client_lock = client_info['lock']

            with client_lock:
                try:
                    logger.info(f'[{user_id}] Sending query result to client.')
                    send_message(client_socket, payload)
                    # It is going to be deleted on cleanup
                    # self.pending_results_wal_manager.remove_entry(wal_filepath)

                    def increment_queries(state):
                        state['queries_received'] += 1

                    self._update_client_state(user_id, increment_queries)

                    with self.clients_lock:
                        final_count = self.clients[user_id]['queries_received']

                    if final_count >= self.expected_query_count:
                        logger.info(
                            f'[{user_id}] All expected query results received. Cleaning up client.'
                        )
                        self._cleanup_client(user_id)
                        # Si se cae acá, caso ultra borde, se pierde todo lo que el cliente
                        # no haya leído. No hago ACK para no tener bloqueada la cola de results.
                        # Tampoco hago que sea bloqueante para que no se mezcle con los acks que
                        # recibe por medio de los datos.
                except Exception as e:
                    logger.error(
                        f'[{user_id}] Error sending query result: {e}', exc_info=True
                    )
                finally:
                    self.send_queue.task_done()

    def shutdown(self):
        if not self.is_running:
            return
        logger.info('Shutting down Cleaner...')
        self.is_running = False

        if self.server_socket:
            try:
                self.server_socket.close()
                logger.info('Server socket closed.')
            except Exception as e:
                logger.warning(f'Error closing server socket: {e}')
            self.server_socket = None

        with self.clients_lock:
            active_user_ids = list(self.clients.keys())

        logger.info(
            f'Found {len(active_user_ids)} active clients to clean up during shutdown.'
        )
        for user_id in active_user_ids:
            self._cleanup_client(user_id)

        if self.connection:
            try:
                logger.info('Closing messaging connection...')
                self.connection.close()
                logger.info('Messaging connection closed.')
            except Exception as e:
                logger.error(f'Error closing messaging connection: {e}', exc_info=True)

        try:
            self.healthcheck.stop()
        except Exception as e:
            logger.error(f'Error stopping healthchecker: {e}')

        logger.info('Cleaner shutdown sequence complete.')

    def _setup_server_socket(self):
        if not self.is_running:
            return False
        try:
            logger.info(f'Setting up server socket on port {self.port}...')
            self.server_socket = create_server_socket(self.port, self.backlog)
            logger.info('Server socket listening.')
            return True
        except Exception as e:
            logger.critical(f'Failed to set up server socket: {e}', exc_info=True)
            self.is_running = False
            return False

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.shutdown()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Signal handlers configured.')

    def _recover_pending_results(self):
        logger.info('Attempting to recover pending results from WAL...')
        recovered_results = self.pending_results_wal_manager.recover()

        for filepath, payload in recovered_results:
            try:
                filename = os.path.basename(filepath)
                user_id_str = filename.split('_')[0]
                user_id = uuid.UUID(user_id_str)
                self.send_queue.put((user_id, payload, filepath))
                logger.info(
                    f'Re-enqueued pending result for user {user_id} from {filename}'
                )

            except (IndexError, ValueError) as e:
                logger.error(
                    f'Failed to parse user_id from pending result file {filepath}: {e}',
                    exc_info=True,
                )
            except Exception as e:
                logger.error(
                    f'Failed to recover pending result from file {filepath}: {e}',
                    exc_info=True,
                )

        logger.info(
            f'Recovered and re-enqueued {len(recovered_results)} pending results.'
        )

    def run(self):
        if not self.is_running:
            logger.critical(
                'Cleaner configuration failed during initialization. Cannot run.'
            )
            return

        self._setup_signal_handlers()
        self._recover_pending_results()

        if not self._setup_server_socket():
            logger.critical('Failed to setup server socket. Cleaner cannot run.')
            return
        self.results_processing_thread.start()
        logger.info('Results processing thread has been started.')
        self.sender_thread.start()
        logger.info('Sender thread has been started.')

        logger.info(
            f'Cleaner server running on port {self.port}. Waiting for client connections...'
        )

        client_threads = []
        try:
            while self.is_running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    if not self.is_running:
                        client_socket.close()
                        break

                    logger.info(
                        f'Accepted new connection from {client_address[0]}:{client_address[1]}'
                    )

                    thread = threading.Thread(
                        target=self._handle_single_client,
                        args=(client_socket, client_address),
                    )
                    thread.daemon = True
                    thread.name = (
                        f'ClientThread-{client_address[0]}-{client_address[1]}'
                    )
                    thread.start()
                    client_threads.append(thread)

                    client_threads = [t for t in client_threads if t.is_alive()]

                except socket.error as e:
                    if self.is_running:
                        logger.error(
                            f'Socket error while accepting connection: {e}',
                            exc_info=True,
                        )
                    else:
                        logger.info('Server socket closed, accept loop terminating.')
                    break
                except Exception as e:
                    if self.is_running:
                        logger.critical(
                            f'Unexpected error in main accept loop: {e}', exc_info=True
                        )

                    break

        except KeyboardInterrupt:
            logger.info('KeyboardInterrupt received in run loop. Initiating shutdown.')
        finally:
            logger.info('Main client accept loop has finished.')
            if self.is_running:
                self.shutdown()

            if self.results_processing_thread.is_alive():
                logger.info('Waiting for results processing thread to complete...')
                self.results_processing_thread.join(timeout=5.0)
                if self.results_processing_thread.is_alive():
                    logger.warning(
                        'Results processing thread did not complete in the allocated time.'
                    )

            for t in client_threads:
                if t.is_alive():
                    t.join(timeout=1.0)
            logger.info('Cleaner has finished its run method.')

    def generate_user_id(self) -> uuid.UUID:
        return uuid.uuid4()


if __name__ == '__main__':
    cleaner_instance = None
    try:
        config = Config()
        initialize_log(config.log_level)
        cleaner_instance = Cleaner(config)
        cleaner_instance.run()
        logger.info('Cleaner run method finished.')
        sys.exit(0)
    except ValueError as e:
        logger.critical(f'Failed to initialize Cleaner: {e}')
        sys.exit(1)
    except Exception as e:
        logger.critical(
            f'Unhandled exception in main execution block: {e}', exc_info=True
        )
        if cleaner_instance and cleaner_instance.is_running:
            cleaner_instance.shutdown()
        sys.exit(1)
