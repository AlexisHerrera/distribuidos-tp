import logging
import os
import queue
import signal
import socket
import sys
import threading
import uuid
from collections import defaultdict
from queue import Queue

from src.common.protocol.batch import Batch, BatchType
from src.common.socket_communication import (
    create_server_socket,
    receive_message,
    send_message,
)
from src.messaging.connection_creator import ConnectionCreator
from src.messaging.protocol.message import Message, MessageType
from src.model.cast import Cast
from src.model.movie import Movie
from src.model.rating import Rating
from src.server.cleaner.clean_credits import parse_line_to_credits
from src.server.cleaner.clean_movies import parse_line_to_movie
from src.server.cleaner.clean_ratings import parse_line_to_rating
from src.server.heartbeat import Heartbeat
from src.utils.config import Config
from src.utils.log import initialize_log

logger = logging.getLogger(__name__)


class Cleaner:
    def __init__(self, config: Config):
        logger.info('Initializing Cleaner...')
        self.config = config
        self.is_running = True
        self.heartbeat: Heartbeat = Heartbeat(config.heartbeat_port)
        self.connection = ConnectionCreator.create_multipublisher(config)
        self.server_socket = None
        self.expected_query_count = 5
        self.client_data = {}
        self.client_data_lock = threading.Lock()
        self.next_user_id = 0
        self.user_id_lock = threading.Lock()
        # Sender Thread
        self.send_queue: Queue[tuple[int, bytes]] = queue.Queue()
        self.sender_thread = threading.Thread(
            target=self._send_loop, name='SenderThread', daemon=True
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

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.shutdown()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Signal handlers configured.')

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

    def batch_to_list_objects(self, batch: Batch) -> list:
        if batch.type == BatchType.MOVIES:
            return self._batch_data_to_movies(batch.data)
        elif batch.type == BatchType.CREDITS:
            return self._batch_data_to_credits(batch.data)
        elif batch.type == BatchType.RATINGS:
            return self._batch_data_to_ratings(batch.data)
        else:
            logger.warning(f'No parser implemented for BatchType: {batch.type.name}')
            return []

    def _batch_data_to_movies(self, data_lines: list[str]) -> list[Movie]:
        parsed_movies = []
        for line in data_lines:
            if not self.is_running:
                break
            if not line.strip():
                continue
            movie = parse_line_to_movie(line)
            if movie:
                parsed_movies.append(movie)

        return parsed_movies

    def _batch_data_to_credits(self, data_lines: list[str]) -> list[Cast]:
        parsed_credits = []
        for line in data_lines:
            if not self.is_running:
                break
            if not line.strip():
                continue
            credits = parse_line_to_credits(line)
            if credits:
                parsed_credits.append(credits)

        return parsed_credits

    def _batch_data_to_ratings(self, data_lines: list[str]) -> list[Rating]:
        parsed_ratings = []
        for line in data_lines:
            if not self.is_running:
                break
            if not line.strip():
                continue
            rating = parse_line_to_rating(line)
            if rating:
                parsed_ratings.append(rating)

        return parsed_ratings

    def _process_client_data(
        self,
        user_id: uuid.UUID,
        client_socket,
        data_type_label: str,
        associated_message_type: MessageType,
    ):
        if not self.is_running or not client_socket:
            logger.error(
                f'[{user_id}] Cannot process client data for {data_type_label}: component missing or not running.'
            )
            return False

        logger.info(f'[{user_id}] Starting to process {data_type_label} from client...')
        processed_counts = defaultdict(int)
        total_batches = 0

        while self.is_running:
            try:
                raw_batch_bytes = receive_message(client_socket)
                if raw_batch_bytes is None:
                    logger.warning(
                        f'[{user_id}] Client connection closed or receive failed while waiting for {data_type_label} batch.'
                    )
                    return False

                batch = Batch.from_bytes(raw_batch_bytes)
                total_batches += 1
                if batch is None:
                    logger.error(
                        f'[{user_id}] Failed to decode Batch #{total_batches} for {data_type_label}.'
                    )
                    return False

                logger.debug(
                    f'[{user_id}] Received Batch #{total_batches} ({data_type_label}): Type={batch.type.name}, Lines={len(batch.data)}'
                )

                if batch.type == BatchType.EOF:
                    logger.info(
                        f'[{user_id}] EOF Batch received for {data_type_label}. End of this stream.'
                    )
                    self._publish_eof(user_id, associated_message_type)
                    break

                object_list = self.batch_to_list_objects(batch)
                if not associated_message_type:
                    logger.warning(
                        f'[{user_id}] No target queue or message type defined for BatchType: {batch.type.name}. Skipping batch.'
                    )
                    continue

                model_object_list = [obj for obj in object_list if self.is_running]
                if not self.is_running and model_object_list:
                    logger.info(
                        f'[{user_id}] Server shutting down, aborting send for batch of {associated_message_type.name}'
                    )
                    return False

                if model_object_list:
                    output_message = Message(
                        user_id,
                        associated_message_type,
                        model_object_list,
                        message_id=None,
                    )

                    self.connection.send(output_message)
                    processed_counts[batch.type] += len(model_object_list)
                    logger.debug(
                        f'[{user_id}] Published {len(model_object_list)} objects of type {batch.type.name} from Batch #{total_batches}'
                    )

            except ConnectionResetError:
                logger.warning(
                    f'[{user_id}] Client connection reset during {data_type_label} processing.'
                )
                return False
            except ConnectionAbortedError:
                logger.warning(
                    f'[{user_id}] Client connection aborted during {data_type_label} processing.'
                )
                return False
            except socket.error as e:
                logger.warning(
                    f'[{user_id}] Socket error during {data_type_label} processing: {e}'
                )
                return False
            except Exception as e:
                if self.is_running:
                    logger.error(
                        f'[{user_id}] Unexpected error in {data_type_label} receive loop: {e}',
                        exc_info=True,
                    )
                return False

        logger.info(
            f'[{user_id}] Client data processing for {data_type_label} finished.'
        )
        logger.info(
            f'[{user_id}] Processing Summary for {data_type_label}: {dict(processed_counts)}'
        )
        return True

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

    def _process_message_from_queue(self, message: Message):
        user_id = message.user_id

        with self.client_data_lock:
            client_info = self.client_data.get(user_id)

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
        self.send_queue.put((user_id, payload))

        logger.info(f'[{user_id}] Enqueued {code} for sending')

    def _send_loop(self):
        while self.is_running or not self.send_queue.empty():
            try:
                user_id, payload = self.send_queue.get(timeout=1)
            except queue.Empty:
                continue

            with self.client_data_lock:
                client_info = self.client_data.get(user_id)
            if not client_info:
                logger.debug(
                    f'[{user_id}] Client disconnected before sending query result'
                )
                self.send_queue.task_done()
                continue

            client_socket = client_info['socket']
            client_lock = client_info['lock']

            with client_lock:
                try:
                    logger.info(f'[{user_id}] Sending query result to client.')
                    send_message(client_socket, payload)
                    client_info['queries_received'] += 1
                    if client_info['queries_received'] >= self.expected_query_count:
                        self._cleanup_client(user_id)
                except Exception as e:
                    logger.error(
                        f'[{user_id}] Error sending query result: {e}', exc_info=True
                    )
                    self._cleanup_client(user_id)
                finally:
                    self.send_queue.task_done()

    def _handle_single_client(self, client_socket, client_address):
        user_id = self.generate_user_id()

        logger.info(
            f'[{user_id}] New connection accepted from {client_address[0]}:{client_address[1]}. Handling in thread: {threading.current_thread().name}'
        )

        with self.client_data_lock:
            self.client_data[user_id] = {
                'socket': client_socket,
                'queries_received': 0,
                'lock': threading.Lock(),
            }

        all_data_processed_successfully = True
        try:
            if all_data_processed_successfully:
                all_data_processed_successfully = self._process_client_data(
                    user_id, client_socket, 'MOVIES', MessageType.Movie
                )

            if all_data_processed_successfully and self.is_running:
                all_data_processed_successfully = self._process_client_data(
                    user_id, client_socket, 'CREDITS', MessageType.Cast
                )

            if all_data_processed_successfully and self.is_running:
                all_data_processed_successfully = self._process_client_data(
                    user_id, client_socket, 'RATINGS', MessageType.Rating
                )

            if not self.is_running:
                logger.info(
                    f'[{user_id}] Server shutdown initiated. Aborting further processing for this client.'
                )
                all_data_processed_successfully = False

            if all_data_processed_successfully:
                logger.info(
                    f'[{user_id}] All data streams received from client and corresponding EOFs published to message queue. Client connection remains open, awaiting query results.'
                )
            else:
                logger.warning(
                    f'[{user_id}] Data processing failed or was incomplete for one or more streams. Cleaning up client.'
                )
                self._cleanup_client(user_id)

        except Exception as e:
            logger.error(
                f'[{user_id}] Unhandled error in client handler for {client_address[0]}:{client_address[1]}: {e}',
                exc_info=True,
            )
            if self.is_running:
                self._cleanup_client(user_id)
        finally:
            logger.debug(
                f'[{user_id}] Client handler thread for {client_address[0]}:{client_address[1]} is ending.'
            )

    def _cleanup_client(self, user_id: uuid.UUID):
        logger.info(f'[{user_id}] Cleaning up client resources.')
        with self.client_data_lock:
            client_info = self.client_data.pop(user_id, None)

        if client_info:
            client_socket = client_info['socket']
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except (socket.error, OSError):
                pass
            try:
                client_socket.close()
                logger.info(f'[{user_id}] Client socket closed.')
            except (socket.error, OSError) as e:
                logger.warning(f'[{user_id}] Error closing client socket: {e}')
        else:
            logger.warning(
                f'[{user_id}] Attempted to clean up client, but not found in active list.'
            )

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

        with self.client_data_lock:
            active_user_ids = list(self.client_data.keys())

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
            self.heartbeat.stop()
        except Exception as e:
            logger.error(f'Error stopping heartbeater: {e}')

        logger.info('Cleaner shutdown sequence complete.')

    def run(self):
        if not self.is_running:
            logger.critical(
                'Cleaner configuration failed during initialization. Cannot run.'
            )
            return

        self._setup_signal_handlers()

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
        with self.user_id_lock:
            user_id = uuid.uuid4()
        return user_id


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
