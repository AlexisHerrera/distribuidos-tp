import argparse
import io
import logging
import os
import signal
import socket
import sys
import threading
import uuid
from queue import Empty, Queue

from src.client.ClientStateMachine import ClientState, ClientStateMachine
from src.common.protocol.batch import Batch, BatchType
from src.common.socket_communication import (
    connect_to_server,
    receive_message,
    send_message,
)
from src.messaging.protocol.message import Message, MessageType
from src.model.actor_count import ActorCount
from src.model.movie import Movie
from src.model.movie_avg_budget import MovieAvgBudget
from src.model.movie_budget_counter import MovieBudgetCounter
from src.model.movie_rating_avg import MovieRatingAvg

BATCH_SIZE_MOVIES = int(os.getenv('BATCH_SIZE_MOVIES', '20'))
BATCH_SIZE_RATINGS = int(os.getenv('BATCH_SIZE_RATINGS', '200'))
BATCH_SIZE_CREDITS = int(os.getenv('BATCH_SIZE_CREDITS', '20'))
WINDOW_SIZE = int(os.getenv('WINDOW_SIZE', '100'))

RESULTS_FOLDER = '.results'
Q_RESULT_FILE_NAME_TEMPLATE = RESULTS_FOLDER + '/user_{user_id}_Q{n}.txt'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Reads movie, rating, and cast files in chunks and simulates sending data.',
        epilog='Example: python main.py movies.csv data/ratings.dat ./casts.txt',
    )
    parser.add_argument(
        'movies_path',
        metavar='<path_movies>',
        type=argparse.FileType('r', encoding='utf-8'),
        help='Path to the movies data file.',
    )
    parser.add_argument(
        'ratings_path',
        metavar='<path_ratings>',
        type=argparse.FileType('r', encoding='utf-8'),
        help='Path to the ratings data file.',
    )
    parser.add_argument(
        'cast_path',
        metavar='<path_cast>',
        type=argparse.FileType('r', encoding='utf-8'),
        help='Path to the cast data file.',
    )

    args = parser.parse_args()

    args.movies_path_name = args.movies_path.name
    args.ratings_path_name = args.ratings_path.name
    args.cast_path_name = args.cast_path.name

    return args


def read_next_batch(csv_file, batch_size):
    batch = []
    for _ in range(batch_size):
        line = csv_file.readline()
        if not line:
            break
        batch.append(line.strip())
    return batch


def _peek_and_count_batches(
    file_object: io.TextIOWrapper, batch_size: int, max_batches: int
):
    initial_pos = file_object.tell()
    batch_count = 0
    eof_reached = False
    try:
        while batch_count < max_batches:
            lines = read_next_batch(file_object, batch_size)
            if not lines:
                eof_reached = True
                break
            batch_count += 1
    finally:
        file_object.seek(initial_pos)

    return batch_count, eof_reached


class Client:
    def __init__(self, args):
        self.args = args
        self.client_socket = None
        self.session_id = None

        self.ack_queue = Queue()
        # Socket management (wait restart)
        self._socket_lock = threading.Lock()
        # Only signaled when everything has ended
        self._shutdown_event = threading.Event()
        self._connection_ready = threading.Event()
        self._reconnect_needed = threading.Event()

        self.results_received = 0
        self.TOTAL_RESULTS_EXPECTED = 5

        self.file_offsets = {
            ClientState.SENDING_MOVIES: 0,
            ClientState.SENDING_CREDITS: 0,
            ClientState.SENDING_RATINGS: 0,
        }

        batch_sizes = {
            'MOVIES': BATCH_SIZE_MOVIES,
            'CREDITS': BATCH_SIZE_CREDITS,
            'RATINGS': BATCH_SIZE_RATINGS,
        }
        self.state_machine = ClientStateMachine(args=args, BATCH_SIZES=batch_sizes)

        self.sender_thread = threading.Thread(
            target=self._sender_loop, name='SenderThread'
        )
        self.receiver_thread = threading.Thread(
            target=self._receiver_loop, name='ReceiverThread'
        )
        self.connection_thread = threading.Thread(
            target=self._connection_manager_loop, name='ConnectionManagerThread'
        )

    def run(self):
        try:
            self._initial_connect()

            self.connection_thread.start()
            self.receiver_thread.start()
            self.sender_thread.start()

            self.sender_thread.join()
            self.receiver_thread.join()
            self.connection_thread.join()

            if self._shutdown_event.is_set():
                logging.info('Client was shut down prematurely.')
            else:
                logging.info('Client work completed successfully.')
        except Exception as e:
            logging.error(
                f'A critical error occurred in the main thread: {e}', exc_info=True
            )
        finally:
            self.stop()
            logging.info('Client has finished.')

    def stop(self):
        if not self._shutdown_event.is_set():
            logging.info('Client shutdown sequence initiated.')
            self._shutdown_event.set()
            self._reconnect_needed.set()
            self._connection_ready.set()
            if self.client_socket:
                try:
                    self.client_socket.close()
                except socket.error:
                    pass

    def _trigger_reconnection(self):
        with self._socket_lock:
            if self._connection_ready.is_set():
                logging.info('Signaling connection loss to Connection Manager.')
                self._connection_ready.clear()
                self._reconnect_needed.set()

    def _connection_manager_loop(self):
        logging.info('Connection Manager started, waiting for reconnect signals.')
        while not self._shutdown_event.is_set():
            self._reconnect_needed.wait()

            if self._shutdown_event.is_set():
                break

            logging.info(
                'Connection Manager received signal, starting reconnection process.'
            )
            self._reconnect_logic()

            # Clean signal for next use
            self._reconnect_needed.clear()

        logging.info('Connection Manager has finished.')

    def _reconnect_logic(self):
        retry_delay = float(os.getenv('CONNECT_RETRY_DELAY', '5.0'))
        while not self._shutdown_event.is_set():
            logging.info('Attempting to reconnect...')
            new_socket = self._create_client_socket()
            if new_socket:
                try:
                    self.session_id = self._perform_handshake(
                        new_socket, self.session_id
                    )
                    with self._socket_lock:
                        self.client_socket = new_socket
                    self._connection_ready.set()
                    logging.info('Reconnection successful.')
                    return
                except ConnectionError as e:
                    logging.warning(f'Handshake failed during reconnect: {e}')
                    try:
                        new_socket.close()
                    except socket.error:
                        pass

            logging.warning(
                f'Reconnect attempt failed. Retrying in {retry_delay} seconds...'
            )
            self._shutdown_event.wait(timeout=retry_delay)

    def _sender_loop(self):
        while (
            not self._shutdown_event.is_set()
            and self.state_machine.get_current_stage()
            not in [ClientState.WAITING_RESULTS, ClientState.DONE]
        ):
            try:
                self._connection_ready.wait()
                current_state = self.state_machine.get_current_stage()
                current_config = self.state_machine.get_current_config()

                if current_config:
                    self._send_file_data(
                        current_state,
                        current_config['file'],
                        current_config['batch_size'],
                    )
                    self.state_machine.advance()

            except ConnectionError:
                logging.warning(
                    'Sender detected a connection error. Signaling thread to reconnect'
                )
                self._trigger_reconnection()

        if not self._shutdown_event.is_set():
            logging.info('Sender thread has sent all files and is now finished.')

    def _receiver_loop(self):
        while (
            not self._shutdown_event.is_set()
            and self.state_machine.get_current_stage() != ClientState.DONE
        ):
            self._connection_ready.wait()
            try:
                raw_message = receive_message(self.client_socket)
                if not raw_message:
                    if not self._shutdown_event.is_set():
                        raise ConnectionError('Connection closed by server.')
                    break

                msg = Message.from_bytes(raw_message)

                if msg.message_type == MessageType.ACK:
                    self.ack_queue.put(msg)
                else:
                    self._process_result_message(msg)

            except (
                ConnectionError,
                ConnectionResetError,
                socket.timeout,
                socket.error,
            ) as e:
                if not self._shutdown_event.is_set():
                    logging.warning(
                        f'Receiver detected connection loss: {e}. Notifying Connection Manager.'
                    )
                    self._trigger_reconnection()
            except Exception as e:
                if not self._shutdown_event.is_set():
                    logging.error(
                        f'An unexpected error occurred in receiver loop: {e}',
                        exc_info=True,
                    )
                    self.stop()
                    break
        logging.info('Receiver thread has finished.')

    def _send_file_data(
        self, current_state: ClientState, file_object: io.TextIOWrapper, batch_size
    ):
        file_description = current_state.name
        logging.info(
            f'--- Starting to send {file_description} data using windowing ---'
        )
        last_confirmed_offset = self.file_offsets[current_state]
        file_object.seek(last_confirmed_offset)

        # Ignore header
        if last_confirmed_offset == 0:
            header = file_object.readline()
            if not header:
                logging.warning(f'File for {file_description} is empty.')
                # Aún así, envía un EOF para que el servidor sepa que no hay datos.
                self._send_window_and_wait_ack([], send_eof=True)
                return
            self.file_offsets[current_state] = file_object.tell()

        while not self._shutdown_event.is_set():
            # Peek how many can be sent in this window
            batches_in_window, eof_in_window = _peek_and_count_batches(
                file_object, batch_size, WINDOW_SIZE
            )
            if batches_in_window == 0 and not eof_in_window:
                logging.info(f'[{file_description}] No more data to send.')
                break

            # Read batches and append
            batches_to_send = []
            for _ in range(batches_in_window):
                batch_lines = read_next_batch(file_object, batch_size)
                if batch_lines:
                    batches_to_send.append(batch_lines)

            offset_after_window = file_object.tell()
            config = self.state_machine.get_current_config()
            batch_type = config['batch_type']
            batch_objects = [Batch(batch_type, data) for data in batches_to_send]

            logging.info(
                f'[{file_description}] Preparing window with {len(batch_objects)} data batches. EOF in window: {eof_in_window}'
            )
            self._send_window_and_wait_ack(batch_objects, eof_in_window)

            # Update offset only if acked
            self.file_offsets[current_state] = offset_after_window
            logging.info(
                f'[{file_description}] Window sent and ACKed. New file offset: {offset_after_window}'
            )

            if eof_in_window:
                logging.info(
                    f'--- Finished sending {file_description} data (EOF sent). ---'
                )
                break

    def _send_window_and_wait_ack(self, batches: list[Batch], send_eof: bool):
        max_retries = int(os.getenv('SEND_MAX_RETRIES', '5'))
        ack_timeout = float(os.getenv('ACK_TIMEOUT', '10.0'))

        for attempt in range(max_retries):
            self._connection_ready.wait()
            try:
                messages_to_send_bytes = [b.to_bytes() for b in batches]
                if send_eof:
                    eof_batch = Batch(BatchType.EOF, None)
                    messages_to_send_bytes.append(eof_batch.to_bytes())

                if not messages_to_send_bytes:
                    return

                window_size = len(messages_to_send_bytes)
                window_message = Message(
                    self.session_id, MessageType.WINDOW, str(window_size)
                )

                with self._socket_lock:
                    send_message(self.client_socket, window_message.to_bytes())
                    for msg_bytes in messages_to_send_bytes:
                        send_message(self.client_socket, msg_bytes)

                # Wait ACK for window
                ack_msg = self.ack_queue.get(timeout=ack_timeout)
                if ack_msg and ack_msg.message_type == MessageType.ACK:
                    logging.info(f'Window ACK received for {window_size} messages.')
                    return
            except Empty:
                logging.warning(
                    f'ACK for window not received in {ack_timeout}s (Attempt {attempt + 1}/{max_retries}).'
                )
                self._trigger_reconnection()
                continue
            except (socket.error, BrokenPipeError, ConnectionError) as e:
                logging.error(
                    f'Connection error on sending window: {e}. Notifying Connection Manager.'
                )
                self._trigger_reconnection()
        raise ConnectionError(
            f'Failed to send window and receive ACK after {max_retries} attempts.'
        )

    def _initial_connect(self):
        self.client_socket = self._create_client_socket()
        if self.client_socket:
            self.session_id = self._perform_handshake(self.client_socket, None)
            self._connection_ready.set()
        else:
            logging.critical('Could not connect to server on startup. Exiting.')
            raise ConnectionError('Failed to establish initial connection.')

    def _reconnect_loop(self):
        retry_delay = float(os.getenv('CONNECT_RETRY_DELAY', '5.0'))
        while not self._shutdown_event.is_set():
            logging.info('Attempting to reconnect...')
            new_socket = self._create_client_socket()
            if new_socket:
                try:
                    self.session_id = self._perform_handshake(
                        new_socket, self.session_id
                    )
                    self.client_socket = new_socket
                    self._connection_ready.set()
                    logging.info('Reconnection successful.')
                    return
                except ConnectionError as e:
                    logging.warning(f'Handshake failed during reconnect: {e}')
                    try:
                        new_socket.close()
                    except socket.error:
                        pass

            logging.warning(
                f'Reconnect attempt failed. Retrying in {retry_delay} seconds...'
            )
            # Espera para el siguiente reintento, pero se interrumpe si el cliente se apaga.
            self._shutdown_event.wait(timeout=retry_delay)

    def _process_result_message(self, msg: Message):
        filename_args = {'user_id': msg.user_id}
        # Q1: Movie
        if msg.message_type == MessageType.Movie:
            movies: list[Movie] = msg.data
            filename_args['n'] = 1
            filename = Q_RESULT_FILE_NAME_TEMPLATE.format(**filename_args)
            logging.info(f'Persisting results Q1 to {filename}...')
            with open(filename, 'w', encoding='utf-8') as f:
                if len(movies) == 0:
                    f.write('[Q1] No movies to show.\n')
                else:
                    for m in movies:
                        f.write(
                            f'[Q1] Movie → ID={m.id}, Title="{m.title}", Genres="{m.genres}"\n'
                        )

        # Q2: MovieBudgetCounter
        elif msg.message_type == MessageType.MovieBudgetCounter:
            data: list[MovieBudgetCounter] = msg.data
            filename_args['n'] = 2
            filename = Q_RESULT_FILE_NAME_TEMPLATE.format(**filename_args)
            logging.info(f'Persisting results Q2 to {filename}...')
            with open(filename, 'w', encoding='utf-8') as f:
                if len(data) == 0:
                    f.write('[Q2] No movies to show.\n')
                else:
                    for position, c in enumerate(data):
                        f.write(
                            f'[Q2] {position + 1}. Country="{c.country}", TotalBudget={c.total_budget}\n'
                        )

        # Q3: MovieRatingAvg
        elif msg.message_type == MessageType.MovieRatingAvg:
            ratings_dict: dict[str, MovieRatingAvg] = msg.data
            filename_args['n'] = 3
            filename = Q_RESULT_FILE_NAME_TEMPLATE.format(**filename_args)
            min_movie = ratings_dict.get('min')
            max_movie = ratings_dict.get('max')
            logging.info(f'Persisting results Q3 to {filename}...')
            with open(filename, 'w', encoding='utf-8') as f:
                if not min_movie and not max_movie:
                    f.write('[Q3] No movies to show.\n')
                if min_movie:
                    f.write(
                        f'[Q3 - MÍN] Película: "{min_movie.title}", Rating={min_movie.average_rating:.2f}\n'
                    )
                if max_movie:
                    f.write(
                        f'[Q3 - MÁX] Película: "{max_movie.title}", Rating={max_movie.average_rating:.2f}\n'
                    )

        # Q4: ActorCount
        elif msg.message_type == MessageType.ActorCount:
            counts: list[ActorCount] = msg.data
            filename_args['n'] = 4
            filename = Q_RESULT_FILE_NAME_TEMPLATE.format(**filename_args)
            logging.info(f'Persisting results Q4 to {filename}...')
            with open(filename, 'w', encoding='utf-8') as f:
                if len(counts) == 0:
                    f.write('[Q4] No actors to show.\n')
                else:
                    for ac in counts:
                        f.write(f'[Q4] Actor="{ac.actor_name}", Count={ac.count}\n')

        # Q5: MovieAvgBudget
        elif msg.message_type == MessageType.MovieAvgBudget:
            mab: MovieAvgBudget = msg.data
            filename_args['n'] = 5
            filename = Q_RESULT_FILE_NAME_TEMPLATE.format(**filename_args)
            logging.info(f'Persisting results Q5 to {filename}...')
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(
                    f'[Q5] MovieAvgBudget → Positive={mab.positive:.2f}, Negative={mab.negative:.2f}\n'
                )
        else:
            logging.warning(f'Mensaje desconocido: {msg.message_type}')

        self.results_received += 1
        logging.info(
            f'Result #{self.results_received}/{self.TOTAL_RESULTS_EXPECTED} received.'
        )
        if self.results_received >= self.TOTAL_RESULTS_EXPECTED:
            logging.info('All expected results have been received. Client is done.')
            self.state_machine.set_stage(ClientState.DONE)

    def _create_client_socket(self):
        try:
            server_host = os.getenv('SERVER_HOST', 'cleaner')
            server_port = int(os.getenv('SERVER_PORT', '12345'))
            max_retries = int(os.getenv('CONNECT_MAX_RETRIES', '5'))
            retry_delay = float(os.getenv('CONNECT_RETRY_DELAY', '3.0'))
        except ValueError as e:
            logging.error(f'Invalid connection parameter environment variable: {e}')
            return None

        for attempt in range(max_retries):
            logging.info(
                f'Attempting to connect to {server_host}:{server_port} (Attempt {attempt + 1}/{max_retries})...'
            )
            client_socket = connect_to_server(server_host, server_port)

            if client_socket:
                logging.info('Connection successful.')
                return client_socket

            logging.warning(f'Connection attempt {attempt + 1} failed.')
            if attempt < max_retries - 1:
                logging.info(f'Retrying in {retry_delay} seconds...')
                self._shutdown_event.wait(timeout=retry_delay)
                if self._shutdown_event.is_set():
                    return None
            else:
                logging.error(f'Connection failed after {max_retries} attempts.')

        return None

    def _perform_handshake(
        self, client_socket: socket.socket, session_id: uuid.UUID | None
    ) -> uuid.UUID:
        try:
            logging.info(
                f'Performing handshake. Session ID: {"NEW" if not session_id else session_id}'
            )
            handshake_id = session_id or uuid.UUID(int=0)

            request_msg = Message(handshake_id, MessageType.HANDSHAKE_SESSION)
            with self._socket_lock:
                send_message(client_socket, request_msg.to_bytes())
                response_raw = receive_message(client_socket)

            if not response_raw:
                raise ConnectionError('Socket closed during handshake response.')
            response_msg = Message.from_bytes(response_raw)

            if response_msg.message_type == MessageType.HANDSHAKE_SESSION:
                final_session_id = response_msg.user_id
                logging.info(
                    f'Handshake successful. Session established with ID: {final_session_id}'
                )
                return final_session_id
            else:
                raise ConnectionError(
                    f'Handshake failed. Expected HANDSHAKE_SESSION, got {response_msg.message_type}'
                )

        except (socket.error, ConnectionError) as e:
            raise ConnectionError(f'Failed to perform handshake: {e}') from e


def main():
    try:
        args = parse_arguments()
        print('Files found:')
        print(f'  Movies:  {args.movies_path_name}')
        print(f'  Ratings: {args.ratings_path_name}')
        print(f'  Cast:    {args.cast_path_name}')
    except Exception as e:
        print(f'Error during argument parsing: {e}', file=sys.stderr)
        sys.exit(1)

    client = Client(args)

    def handle_signal(_sig, _frame):
        logging.info('Signal received, stopping client...')
        client.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    client.run()


if __name__ == '__main__':
    main()
