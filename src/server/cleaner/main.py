import logging
import os
from common.socket_communication import receive_message, accept_new_connection, create_server_socket

def main():
    logging.basicConfig(level=logging.INFO)

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
    client_sock = accept_new_connection(server_socket)
    if client_sock:
        message = receive_message(client_sock)
        logging.info(f'New message arrived: {message}')

if __name__ == "__main__":
    main()
