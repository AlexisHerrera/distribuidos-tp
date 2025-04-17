import logging
import os
from common.socket_communication import receive_message, accept_new_connection, create_server_socket
from model.movie import Movie
from io import StringIO
import csv
import ast


logging.basicConfig(level=logging.INFO)

def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)
        return [data['name'] for data in dictionary_list]
    except (ValueError, SyntaxError):
        return []

def parse_movie(line: str) -> Movie:
    try:
        csv_reader = csv.reader(StringIO(line), skipinitialspace=True)
        fields = next(csv_reader)

        if len(fields) < 24:
            raise ValueError("Line does not contain enough fields to parse.")

        movie_id = int(fields[5])
        title = fields[20].strip()
        genres = dictionary_to_list(fields[3].strip())
        production_countries = dictionary_to_list(fields[13].strip())
        release_date = fields[14].strip()
        budget = int(fields[2])
        revenue = int(fields[15])
        overview = fields[10].strip()

        return Movie(
            movie_id=movie_id,
            title=title,
            genres=genres,
            release_date=release_date,
            production_countries=production_countries,
            budget=budget,
            revenue=revenue,
            overview=overview
        )

    except Exception as e:
        logging.warning(f"Failed to parse line: {line}. Error: {e}")
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
