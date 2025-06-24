import argparse
import io
import logging
import os
import signal
import socket
import sys
import threading
import time
import uuid
from queue import Empty, Queue

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

RESULTS_FOLDER = '.results'
Q_RESULT_FILE_NAME_TEMPLATE = RESULTS_FOLDER + '/user_{user_id}_Q{n}.txt'

logging.basicConfig(level=logging.INFO)


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


class Client:
    def __init__(self, args):
        self.args = args
        self.client_socket = None
        self.session_id = None
        self.is_running = True

        self.ack_queue = Queue()

        self.sender_thread = threading.Thread(
            target=self._sender_loop, name='SenderThread'
        )
        self.receiver_thread = threading.Thread(
            target=self._receiver_loop, name='ReceiverThread'
        )

    def run(self):
        try:
            self.client_socket = self._create_client_socket()
            if not self.client_socket:
                logging.critical('Could not connect to server. Exiting.')
                return

            self.session_id = self._perform_handshake(self.client_socket, None)

            self.receiver_thread.start()
            self.sender_thread.start()

            self.sender_thread.join()
            self.receiver_thread.join()

        except (ConnectionError, ValueError) as e:
            logging.error(f'A critical error occurred: {e}', exc_info=True)
        finally:
            self.stop()
            logging.info('Client has finished.')

    def stop(self):
        self.is_running = False
        if self.client_socket:
            try:
                self.client_socket.close()
            except socket.error:
                pass
        logging.info('Client shutdown sequence initiated.')

    def _sender_loop(self):
        try:
            self._send_file_data(
                BatchType.MOVIES, self.args.movies_path, BATCH_SIZE_MOVIES
            )
            self._send_file_data(
                BatchType.CREDITS, self.args.cast_path, BATCH_SIZE_CREDITS
            )
            self._send_file_data(
                BatchType.RATINGS, self.args.ratings_path, BATCH_SIZE_RATINGS
            )

            logging.info('All data files have been sent.')
        except ConnectionError as e:
            logging.critical(
                f'Sender thread failed due to connection error: {e}. Stopping client.'
            )
            self.stop()

    def _receiver_loop(self):
        logging.info('Receiver thread started. Waiting for messages from server...')
        while self.is_running:
            try:
                raw_message = receive_message(self.client_socket)
                if not raw_message:
                    logging.info(
                        'Connection closed by server (receive returned nothing).'
                    )
                    break

                msg = Message.from_bytes(raw_message)

                if msg.message_type == MessageType.ACK:
                    logging.debug('ACK received from server, dispatching to sender.')
                    self.ack_queue.put(msg)
                elif msg.message_type == MessageType.EOF and not msg.data:
                    logging.info(
                        'Main EOF received from server. All results are in. Shutting down receiver.'
                    )
                    break
                else:
                    self._process_result_message(msg)
            except (ConnectionError, ConnectionResetError):
                if self.is_running:
                    logging.warning('Connection lost. Receiver thread stopping.')
                break
            except Exception as e:
                if self.is_running:
                    logging.error(
                        f'An error occurred in receiver loop: {e}', exc_info=True
                    )
                break

        logging.info('Receiver thread has finished.')
        self.stop()

    def _send_file_data(
        self, batch_type: BatchType, file_object: io.TextIOWrapper, batch_size
    ):
        file_description = batch_type.name
        logging.info(f'--- Starting to send {file_description} data ---')
        batch_count = 0
        try:
            header = file_object.readline()
            if not header:
                logging.warning(f'File for {file_description} is empty.')
                return

            while self.is_running:
                batch_lines = read_next_batch(file_object, batch_size=batch_size)
                if not batch_lines:
                    break

                batch_obj = Batch(batch_type, batch_lines)
                self._send_with_retry_and_ack(batch_obj.to_bytes())
                batch_count += 1

            logging.info(
                f'--- Finished sending {file_description} data, sending EOF. ---'
            )
            logging.info(f'Batchs sents: {batch_count}')
            eof_batch = Batch(BatchType.EOF, None)
            self._send_with_retry_and_ack(eof_batch.to_bytes())
            logging.info(f'Final EOF for {file_description} sent and ACKed.')

        except Exception:
            raise

    def _send_with_retry_and_ack(self, data_bytes: bytes):
        max_retries = int(os.getenv('SEND_MAX_RETRIES', '5'))
        retry_delay = float(os.getenv('SEND_RETRY_DELAY', '5.0'))
        ack_timeout = 10.0

        for _ in range(max_retries):
            if not self.is_running:
                raise ConnectionError('Client is shutting down.')

            try:
                send_message(self.client_socket, data_bytes)

                ack_msg = self.ack_queue.get(timeout=ack_timeout)

                if ack_msg.message_type == MessageType.ACK:
                    logging.debug('ACK received from queue.')
                    return

            except Empty:
                logging.warning(
                    f'ACK not received within {ack_timeout}s. Retrying send...'
                )
            except (ConnectionError, BrokenPipeError, socket.error) as e:
                logging.error(f'Connection error on send: {e}. Attempting reconnect...')
                self._reconnect()

            time.sleep(retry_delay)

        raise ConnectionError(
            f'Failed to send data and receive ACK after {max_retries} attempts.'
        )

    def _reconnect(self):
        if self.client_socket:
            try:
                self.client_socket.close()
            except socket.error:
                pass

        self.client_socket = self._create_client_socket()
        if self.client_socket:
            self.session_id = self._perform_handshake(
                self.client_socket, self.session_id
            )
        else:
            raise ConnectionError('Failed to reconnect to the server.')

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
                time.sleep(retry_delay)
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
            send_message(client_socket, request_msg.to_bytes())

            response_raw = receive_message(client_socket)
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

        except (ConnectionError, socket.error):
            raise


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
