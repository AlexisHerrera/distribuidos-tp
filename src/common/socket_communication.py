import logging
import struct
import socket


def connect_to_server(host: str, port: int) -> socket.socket | None:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        logging.info(f'Connected to server at {host}:{port}')
        return s
    except ConnectionRefusedError:
        logging.error('Failed to connect to the server.')
        return None
    except Exception as e:
        logging.error(f'An error occurred while connecting: {e}')
        return None


def create_server_socket(port: int, backlog: int) -> socket.socket:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('', port))
    server_socket.listen(backlog)
    logging.info(
        f'Server socket created, bound to port {port}, listening with backlog {backlog}'
    )
    return server_socket


def accept_new_connection(server_socket):
    try:
        logging.info('Accepting new connections...')
        c, addr = server_socket.accept()
        logging.info(f'New connection arrived! Ip: {addr[0]}')
        return c
    except OSError:
        logging.error('An error occured while accepting new connections!')
        return None


def send_message(sock, message):
    encoded_message = message.encode('utf-8')
    message_length = len(encoded_message)

    length_header = struct.pack('>I', message_length)

    sock.sendall(length_header)
    sock.sendall(encoded_message)


def receive_message(sock):
    length_header = recv_exact(sock, 4)
    if not length_header:
        raise ConnectionError('Failed to receive message length header')

    message_length = struct.unpack('>I', length_header)[0]

    message_bytes = recv_exact(sock, message_length)
    if not message_bytes:
        raise ConnectionError('Failed to receive the full message')

    return message_bytes.decode('utf-8')


def recv_exact(sock, num_bytes):
    buffer = b''
    while len(buffer) < num_bytes:
        chunk = sock.recv(num_bytes - len(buffer))
        if not chunk:
            raise ConnectionError('Socket connection broken during receive')
        buffer += chunk
    return buffer
