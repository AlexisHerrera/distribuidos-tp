import argparse
import io
import logging
import os
import signal
import sys
import time

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
CSV_MOVIES_PATH = '.data/movies_metadata.csv'

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


def send_movies(client_socket, args):
    logging.info('Beginning to send movies...')
    if not send_data(
        client_socket, args.movies_path, BatchType.MOVIES, BATCH_SIZE_MOVIES
    ):
        logging.error('Error sending movies')
    logging.info('Sending final EOF marker...')
    eof_batch = Batch(BatchType.EOF, None)
    eof_bytes = eof_batch.to_bytes()
    send_message(client_socket, eof_bytes)
    logging.info('Final EOF marker sent.')


def send_cast(client_socket, args):
    logging.info('Beginning to send credits...')
    if not send_data(
        client_socket, args.cast_path, BatchType.CREDITS, BATCH_SIZE_CREDITS
    ):
        logging.error('Error sending credits')
    logging.info('Sending final EOF marker...')
    eof_batch = Batch(BatchType.EOF, None)
    eof_bytes = eof_batch.to_bytes()
    send_message(client_socket, eof_bytes)
    logging.info('Final EOF marker sent.')


def send_ratings(client_socket, args):
    logging.info('Beginning to send ratings...')
    if not send_data(
        client_socket, args.ratings_path, BatchType.RATINGS, BATCH_SIZE_RATINGS
    ):
        logging.error('Error sending ratings')
    logging.info('Sending final EOF marker...')
    eof_batch = Batch(BatchType.EOF, None)
    eof_bytes = eof_batch.to_bytes()
    send_message(client_socket, eof_bytes)
    logging.info('Final EOF marker sent.')


def send_data(
    client_socket, file_object: io.TextIOWrapper, batch_type: BatchType, batch_size
):
    if not client_socket:
        logging.error('Cannot send data, client socket is not valid.')
        return False
    if not file_object:
        logging.error('Cannot send data, file object is not valid.')
        return False

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

            if not batch_bytes:
                logging.error(
                    f'Failed to serialize batch {batch_number} for {file_description}. Stopping.'
                )
                return False

            send_message(client_socket, batch_bytes)
            total_lines_sent += len(batch_lines)
            # logging.info(
            #    f'Sent Batch #{batch_number} ({file_description}) with {len(batch_lines)} records.'
            # )

        logging.info(
            f'--- Finished sending {file_description}. Total lines sent: {total_lines_sent} ---'
        )
        return True

    except Exception as e:
        logging.error(
            f'Error while reading or sending batches for {file_description}: {e}',
            exc_info=True,
        )
        return False


def create_client_socket():
    try:
        server_host = os.getenv('SERVER_HOST', 'cleaner')
        server_port = int(os.getenv('SERVER_PORT', '12345'))
        max_retries = int(os.getenv('CONNECT_MAX_RETRIES', '5'))
        retry_delay = float(os.getenv('CONNECT_RETRY_DELAY', '3.0'))
    except ValueError as e:
        logging.error(f'Invalid connection parameter environment variable: {e}')
        return None

    client_socket = None
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

        # Q1: Movie
        if msg.message_type == MessageType.Movie:
            movies: list[Movie] = msg.data
            for m in movies:
                print(f'[Q1] Movie → ID={m.id}, Title="{m.title}", Genres="{m.genres}"')
        # Q2: MovieBudgetCounter
        elif msg.message_type == MessageType.MovieBudgetCounter:
            data: dict[str, int] = msg.data
            counters = [
                MovieBudgetCounter(country, total) for country, total in data.items()
            ]
            for position, c in enumerate(counters):
                print(
                    f'[Q2] {position + 1}. Country="{c.country}", TotalBudget={c.total_budget}'
                )

        # Q3: MovieRatingAvg
        elif msg.message_type == MessageType.MovieRatingAvg:
            ratings_dict: dict[str, MovieRatingAvg] = msg.data
            min_movie = ratings_dict.get('min')
            max_movie = ratings_dict.get('max')

            if min_movie:
                print(
                    f'[Q3 - MÍN] Película: "{min_movie.title}", '
                    f'Rating={min_movie.average_rating:.2f}'
                )
            if max_movie:
                print(
                    f'[Q3 - MÁX] Película: "{max_movie.title}", '
                    f'Rating={max_movie.average_rating:.2f}'
                )
        # Q4
        elif msg.message_type == MessageType.ActorCount:
            counts: list[ActorCount] = msg.data
            for ac in counts:
                print(f'[Q4] Actor="{ac.actor_name}", Count={ac.count}')
        # Q5
        elif msg.message_type == MessageType.MovieAvgBudget:
            mab: MovieAvgBudget = msg.data
            print(
                f'[Q5] MovieAvgBudget → Positive={mab.positive:.2f}, '
                f'Negative={mab.negative:.2f}'
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

    try:
        client_socket = create_client_socket()
        if not client_socket:
            logging.critical(
                'Could not connect to server after multiple retries. Exiting.'
            )
            return

        def handle_signal(sig, frame):
            client_socket.close()

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        send_movies(client_socket, args)
        send_cast(client_socket, args)
        send_ratings(client_socket, args)
        logging.info('Waiting for response.')
        receive_responses(client_socket)
        client_socket.close()
        logging.info('Client socket closed.')
    except ValueError as e:
        logging.error('Could not connect to server', e)
        sys.exit(1)


if __name__ == '__main__':
    main()
