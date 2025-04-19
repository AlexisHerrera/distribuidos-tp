import sys
import argparse
import io
import os
import logging
import time

from src.common.socket_communication import send_message, connect_to_server
from src.common.protocol.batch import BatchType, Batch

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '20'))
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


def send_data(client_socket, file_object: io.TextIOWrapper, batch_type: BatchType):
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
            batch_lines = read_next_batch(file_object, BATCH_SIZE)
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
            logging.info(
                f'Sent Batch #{batch_number} ({file_description}) with {len(batch_lines)} records.'
            )

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
        if not send_data(client_socket, args.movies_path, BatchType.MOVIES):
            logging.error('Error sending movies')
        logging.info('Sending final EOF marker...')
        eof_batch = Batch(BatchType.EOF, None)
        eof_bytes = eof_batch.to_bytes()
        send_message(client_socket, eof_bytes)
        logging.info('Final EOF marker sent.')
        logging.info('Waiting for response.')
        time.sleep(60 * 10)
        client_socket.close()
        logging.info('Client socket closed.')
    except ValueError as e:
        logging.error('Could not connect to server', e)
        sys.exit(1)


if __name__ == '__main__':
    main()
