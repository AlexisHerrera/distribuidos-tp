import logging
import os
import csv
from io import StringIO
from common.socket_communication import receive_message, accept_new_connection, create_server_socket
from model.movie import Movie
from model.rating import Rating
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

def parse_rating(rating_string: str) -> Rating:
    try:
        csv_reader = csv.reader(StringIO(rating_string), skipinitialspace=True)
        fields = next(csv_reader)

        if len(fields) < 3:
            raise ValueError("Line does not contain enough fields to parse a rating.")

        return Rating(
            movie_id=int(fields[1]),
            rating=float(fields[2])
        )
    except Exception as e:
        logging.warning(f"Failed to parse rating line: {rating_string}. Error: {e}")
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


def receive_ratings(client_socket):
    while True:
        message = receive_message(client_socket)
        if message == 'EOF_RATINGS':
            logging.info("EOF_RATINGS message received.")
            break
        else:
            logging.info(f'Ratings Batch Received:\n{message}')
            rating_lines = message.strip().split('\n')
            for line in rating_lines:
                if line.strip():
                    parsed_rating = parse_rating(line)
                    if parsed_rating:
                        logging.info(parsed_rating.rating)
    

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
        # receive_movies(client_socket)
        receive_ratings(client_socket)

if __name__ == "__main__":
    main()
