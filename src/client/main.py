import argparse
import io
import logging
import os
import signal
import socket
import sys
import time
import uuid

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
BATCH_SIZE_RATINGS = int(os.getenv('BATCH_SIZE_RATINGS', '100'))
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


def send_movies(client_socket, session_id, args):
    logging.info('Beginning to send movies...')
    current_socket, current_session_id = client_socket, session_id

    new_socket = send_data(
        client_socket, args.movies_path, BatchType.MOVIES, BATCH_SIZE_MOVIES, session_id
    )
    if not new_socket:
        logging.error('Error sending movies')
        return None, None
    current_socket = new_socket

    logging.info('Sending final EOF marker for movies...')
    eof_batch = Batch(BatchType.EOF, None)
    eof_bytes = eof_batch.to_bytes()
    final_socket, final_session_id = send_with_retry_and_ack(
        current_socket, eof_bytes, current_session_id
    )
    logging.info('Final EOF marker for movies sent and ACKed.')
    return final_socket, final_session_id


def send_cast(client_socket, session_id, args):
    logging.info('Beginning to send credits...')
    current_socket, current_session_id = client_socket, session_id

    new_socket = send_data(
        client_socket, args.cast_path, BatchType.CREDITS, BATCH_SIZE_CREDITS, session_id
    )
    if not new_socket:
        logging.error('Error sending credits')
        return None, None
    current_socket = new_socket

    logging.info('Sending final EOF marker for credits...')
    eof_batch = Batch(BatchType.EOF, None)
    eof_bytes = eof_batch.to_bytes()
    final_socket, final_session_id = send_with_retry_and_ack(
        current_socket, eof_bytes, current_session_id
    )

    logging.info('Final EOF marker for credits sent and ACKed.')
    return final_socket, final_session_id


def send_ratings(client_socket, session_id, args):
    logging.info('Beginning to send ratings...')
    current_socket, current_session_id = client_socket, session_id

    new_socket = send_data(
        client_socket,
        args.ratings_path,
        BatchType.RATINGS,
        BATCH_SIZE_RATINGS,
        session_id,
    )
    if not new_socket:
        logging.error('Error sending ratings')
        return None, None
    current_socket = new_socket

    logging.info('Sending final EOF marker for ratings...')
    eof_batch = Batch(BatchType.EOF, None)
    eof_bytes = eof_batch.to_bytes()
    final_socket, final_session_id = send_with_retry_and_ack(
        current_socket, eof_bytes, current_session_id
    )

    logging.info('Final EOF marker for ratings sent and ACKed.')
    return final_socket, final_session_id


def send_data(
    client_socket: socket.socket,
    file_object: io.TextIOWrapper,
    batch_type: BatchType,
    batch_size,
    session_id: uuid.UUID,
) -> socket.socket | None:
    if not client_socket:
        logging.error('Cannot send data, client socket is not valid.')
        return None
    if not file_object:
        logging.error('Cannot send data, file object is not valid.')
        return None
    current_socket = client_socket
    current_session_id = session_id
    file_description = batch_type.name
    logging.info(f'--- Starting to send {file_description} data ---')
    total_lines_sent = 0
    batch_number = 0

    try:
        header = file_object.readline()
        if not header:
            logging.warning(f'File for {file_description} seems empty.')
            return True

        while True:
            batch_lines = read_next_batch(file_object, batch_size=batch_size)
            if not batch_lines:
                logging.info(f'Finished reading file for {file_description}.')
                break

            batch_number += 1
            batch_obj = Batch(batch_type, batch_lines)
            batch_bytes = batch_obj.to_bytes()

            try:
                current_socket, current_session_id = send_with_retry_and_ack(
                    current_socket, batch_bytes, current_session_id
                )
            except ConnectionError as e:
                logging.critical(
                    f'Failed to send batch for {file_description} after all retries: {e}',
                    exc_info=True,
                )
                return None
            total_lines_sent += len(batch_lines)
            # logging.info(
            #    f'Sent Batch #{batch_number} ({file_description}) with {len(batch_lines)} records.'
            # )

        logging.info(
            f'--- Finished sending {file_description}. Total lines sent: {total_lines_sent} ---'
        )
        return current_socket
    except Exception as e:
        logging.error(
            f'Error while reading or sending batches for {file_description}: {e}',
            exc_info=True,
        )
        return None


def perform_handshake(
    client_socket: socket.socket, session_id: uuid.UUID | None
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


def send_with_retry_and_ack(
    client_socket: socket.socket, data_bytes: bytes, session_id: uuid.UUID
) -> tuple[socket.socket, uuid.UUID]:
    max_retries = int(os.getenv('SEND_MAX_RETRIES', '5'))
    retry_delay = float(os.getenv('SEND_RETRY_DELAY', '5.0'))

    current_socket = client_socket
    current_session_id = session_id

    for attempt in range(max_retries):
        if not current_socket:
            logging.warning('Socket is invalid, attempting to reconnect...')
            current_socket = create_client_socket()
            if current_socket:
                try:
                    current_session_id = perform_handshake(
                        current_socket, current_session_id
                    )
                except ConnectionError as e:
                    logging.error(f'Handshake after reconnect failed: {e}')
                    current_socket.close()
                    current_socket = None

            if not current_socket:
                logging.error(
                    f'Reconnection failed on attempt {attempt + 1}. Retrying in {retry_delay}s...'
                )
                time.sleep(retry_delay)
                continue

        try:
            send_message(current_socket, data_bytes)
            ack_raw = receive_message(current_socket)
            ack_msg = Message.from_bytes(ack_raw)

            if ack_msg.message_type == MessageType.ACK:
                logging.debug('ACK received successfully.')
                return current_socket, current_session_id
            else:
                logging.warning(
                    f'Expected ACK but received {ack_msg.message_type}. Retrying...'
                )

        except (
            ConnectionError,
            ConnectionResetError,
            BrokenPipeError,
            socket.error,
        ) as e:
            logging.error(
                f'Connection error during send/ack: {e}. (Attempt {attempt + 1}/{max_retries})'
            )
            if current_socket:
                try:
                    current_socket.close()
                except socket.error:
                    pass
            current_socket = None

        logging.info(f'Retrying in {retry_delay} seconds...')
        time.sleep(retry_delay)

    raise ConnectionError(
        f'Failed to send data and receive ACK after {max_retries} attempts.'
    )


def create_client_socket():
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


def receive_responses(client_socket):
    logging.info('Esperando respuestas del servidor...')
    while True:
        try:
            raw = receive_message(client_socket)
        except ConnectionError:
            logging.info('Conexión cerrada por servidor.')
            break

        msg = Message.from_bytes(raw)
        if msg.message_type == MessageType.EOF:
            logging.info('EOF recibido. Terminando recepción.')
            break

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

    session_id = None
    client_socket = None
    try:
        client_socket = create_client_socket()
        if not client_socket:
            logging.critical(
                'Could not connect to server after multiple retries. Exiting.'
            )
            return
        session_id = perform_handshake(client_socket, session_id)

        def handle_signal(_sig, _frame):
            if client_socket:
                client_socket.close()
            sys.exit(0)

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        client_socket, session_id = send_movies(client_socket, session_id, args)
        if not client_socket:
            logging.critical('Failed to send movies data. Aborting.')
            return

        client_socket, session_id = send_cast(client_socket, session_id, args)
        if not client_socket:
            logging.critical('Failed to send cast data. Aborting.')
            return

        client_socket, session_id = send_ratings(client_socket, session_id, args)
        if not client_socket:
            logging.critical('Failed to send ratings data. Aborting.')
            return

        logging.info('Waiting for response.')
        receive_responses(client_socket)
        client_socket.close()
        logging.info('Client socket closed.')
    except ValueError as e:
        logging.error('Could not connect to server', e)
        sys.exit(1)


if __name__ == '__main__':
    main()
