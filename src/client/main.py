import sys
import argparse
import io
import os
import logging
from common.socket_communication import send_message, connect_to_server


def count_lines_in_file(file_object: io.TextIOWrapper, file_description: str, file_path_for_msg: str) -> bool:
    print(f"\n--- Counting lines in: {file_description} ({file_path_for_msg}) ---")
    line_count = 0
    try:
        with file_object:
            for _ in file_object:
                line_count += 1
    except IOError as e:
        print("Error reading file:", e)
        return False
    except Exception as e:
        print('Unknown error:', e)
        return False
    print(f"--- Counting ended for {file_description}. Total lines: {line_count} ---")
    return True


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Reads movie, rating, and cast files in chunks and simulates sending data.",
        epilog="Example: python main.py movies.csv data/ratings.dat ./casts.txt"
    )
    parser.add_argument('movies_path',
                        metavar='<path_movies>',
                        type=argparse.FileType('r', encoding='utf-8'),
                        help='Path to the movies data file.')
    parser.add_argument('ratings_path',
                        metavar='<path_ratings>',
                        type=argparse.FileType('r', encoding='utf-8'),
                        help='Path to the ratings data file.')
    parser.add_argument('cast_path',
                        metavar='<path_cast>',
                        type=argparse.FileType('r', encoding='utf-8'),
                        help='Path to the cast data file.')

    args = parser.parse_args()

    args.movies_path_name = args.movies_path.name
    args.ratings_path_name = args.ratings_path.name
    args.cast_path_name = args.cast_path.name

    return args


def send_file_batches():
    server_host = os.getenv('SERVER_HOST', 'cleaner')

    try:
        server_port = int(os.getenv('SERVER_PORT', '12345'))
    except ValueError:
        logging.warning("Invalid SERVER_PORT environment variable. Falling back to default port 12345.")
        server_port = 12345

    client_socket = connect_to_server(server_host, server_port)
    if client_socket:
        message = 'Send Batches from client!'
        send_message(client_socket, message)
        logging.info(f'Sent message: {message}')
        client_socket.close()


def main():
    try:
        args = parse_arguments()
        print("Successfully opened files:")
        print(f"  Movies:  {args.movies_path_name}")
        print(f"  Ratings: {args.ratings_path_name}")
        print(f"  Cast:    {args.cast_path_name}")
    except Exception as e:
        print(f"Error during argument parsing: {e}", file=sys.stderr)
        sys.exit(1)

    files_to_process = [
        (args.movies_path, "Movies", args.movies_path_name),
        (args.ratings_path, "Ratings", args.ratings_path_name),
        (args.cast_path, "Cast", args.cast_path_name)
    ]

    all_successful = True
    for file_obj, description, path_name in files_to_process:
        if not count_lines_in_file(file_obj, description, path_name):
            all_successful = False
            print(f"There was a problem processing the {description} file. "
                  "Check error messages above.")

    send_file_batches()

    if all_successful:
        print("\nSuccessfully completed processing all files.")
        sys.exit(0)
    else:
        print("\nProcessing finished, but errors occurred in one or more files.")
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
