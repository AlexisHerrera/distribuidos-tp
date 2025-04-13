import logging
from common.socket_communication import receive_message, accept_new_connection, create_server_socket

def main():
    logging.basicConfig(level=logging.INFO)
    server_socket = create_server_socket(12345,2)
    client_sock = accept_new_connection(server_socket)
    if client_sock:
        message = receive_message(client_sock)
        logging.info(f'New message arrived: {message}')


if __name__ == "__main__":
    main()
