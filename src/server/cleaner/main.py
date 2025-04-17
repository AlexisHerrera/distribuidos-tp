import logging
import os
import csv
from io import StringIO
from common.socket_communication import receive_message, accept_new_connection, create_server_socket
from model.movie import Movie
from utils.string_utils import dictionary_to_list
from movie_fields import MovieCSVField, MIN_EXPECTED_FIELDS

logging.basicConfig(level=logging.INFO)

def parse_movie(movie_string: str) -> Movie:
    try:
        csv_reader = csv.reader(StringIO(movie_string), skipinitialspace=True)
        fields = next(csv_reader)

        if len(fields) < MIN_EXPECTED_FIELDS:
            raise ValueError("Line does not contain enough fields to parse.")

        return Movie(
            movie_id=int(fields[MovieCSVField.MOVIE_ID]),
            title=fields[MovieCSVField.TITLE].strip(),
            genres=dictionary_to_list(fields[MovieCSVField.GENRES].strip()),
            release_date=fields[MovieCSVField.RELEASE_DATE].strip(),
            production_countries=dictionary_to_list(fields[MovieCSVField.PRODUCTION_COUNTRIES].strip()),
            budget=int(fields[MovieCSVField.BUDGET]),
            revenue=int(fields[MovieCSVField.REVENUE]),
            overview=fields[MovieCSVField.OVERVIEW].strip()
        )

    except Exception as e:
        logging.warning(f"Failed to parse line: {movie_string}. Error: {e}")
        return None


def receive_movies(client_socket):
    while True:
        message = receive_message(client_socket)
        if message == 'EOF_MOVIES':
            logging.info("EOF_MOVIES message received.")
            break
        else:
            logging.info(f'Movie Batch Received:\n{message}')
            movies = message.strip().split('\n')
            for movie in movies:
                parsed_movie = parse_movie(movie)
                if parsed_movie:
                    logging.info(parsed_movie)
    
    client_socket.close()

def main():
    try:
        port = int(os.getenv('SERVER_PORT', '12345'))
    except ValueError:
        logging.warning("Invalid SERVER_PORT environment variable. Falling back to default port 12345.")
        port = 12345

    try:
        backlog = int(os.getenv('LISTENING_BACKLOG', '3'))
    except ValueError:
        logging.warning("Invalid LISTENING_BACKLOG environment variable. Falling back to default backlog 3.")
        backlog = 3

    server_socket = create_server_socket(port, backlog)
    client_socket = accept_new_connection(server_socket)
    
    if client_socket:
        receive_movies(client_socket)

if __name__ == "__main__":
    main()
