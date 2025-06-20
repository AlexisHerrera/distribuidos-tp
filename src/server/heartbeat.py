import logging
import threading
from queue import Empty, SimpleQueue

from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket

RECV_BYTES_AMOUNT = 1
MESSAGE_TO_SEND = b'B'  # Beat
SEND_BYTES_AMOUNT = len(MESSAGE_TO_SEND)

logger = logging.getLogger(__name__)


class Heartbeat:
    def __init__(self, heartbeat_port: int):
        self.socket = ServerSocket(heartbeat_port, 5)
        self.is_running = True
        self.manager_queue: SimpleQueue = SimpleQueue()
        self.clients: dict[tuple, (threading.Thread, TCPSocket)] = {}
        self.heartbeat_thread = threading.Thread(target=self.run)
        self.heartbeat_thread.start()

    def _manage_client(self, socket: TCPSocket, address: tuple):
        try:
            while self.is_running:
                message = socket.recv(RECV_BYTES_AMOUNT)
                logger.debug(f'Received {message}')

                socket.send(MESSAGE_TO_SEND, SEND_BYTES_AMOUNT)
                logger.debug(f'Sent {MESSAGE_TO_SEND}')
        except Exception as e:
            logger.error(f'Error while managing client in heartbeat: {e}')
        finally:
            self.manager_queue.put(address)

    def _clean_clients(self):
        while not self.manager_queue.empty():
            try:
                addr = self.manager_queue.get_nowait()
                (client, socket) = self.clients[addr]

                socket.stop()
                client.join()
            except Empty:
                break

    def run(self):
        try:
            while self.is_running:
                heartbeater_socket, addr = self.socket.accept()
                logger.debug(f'Received new heartbeater from {addr}')

                client = threading.Thread(
                    target=self._manage_client, args=(heartbeater_socket, addr)
                )

                self.clients[addr] = (client, heartbeater_socket)

                client.start()

                self._clean_clients()
        except Exception as e:
            logger.error(f'Heartbeat error: {e}')

    def stop(self):
        logger.info('Stopping heartbeat')
        self.is_running = False
        for _, (client, socket) in self.clients.items():
            socket.stop()
            client.join()
        self.socket.stop()
        self.heartbeat_thread.join()
