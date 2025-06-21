import logging
import threading

from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket
from src.utils.config import Config

RECV_BYTES_AMOUNT = 1
MESSAGE_TO_SEND = b'B'  # Beat
SEND_BYTES_AMOUNT = len(MESSAGE_TO_SEND)

logger = logging.getLogger(__name__)


class Heartbeat:
    def __init__(self, config: Config):
        self.socket = ServerSocket(config.heartbeat_port)
        self.is_running = True
        self.heartbeater: TCPSocket | None = None
        self.heartbeat_thread = threading.Thread(target=self.run)
        self.heartbeat_thread.start()

    def _manage_client(self):
        try:
            while self.is_running:
                message = self.heartbeater.recv(RECV_BYTES_AMOUNT)
                logger.debug(f'Received {message}')

                self.heartbeater.send(MESSAGE_TO_SEND, SEND_BYTES_AMOUNT)
                logger.debug(f'Sent {MESSAGE_TO_SEND}')
        except Exception as e:
            logger.error(f'Error while managing client in heartbeat: {e}')

    def run(self):
        try:
            while self.is_running:
                self.heartbeater, address = self.socket.accept()
                logger.debug(f'Received new heartbeater from {address}')

                self._manage_client()
                self.heartbeater.stop()
        except Exception as e:
            logger.error(f'Heartbeat error: {e}')

    def stop(self):
        logger.info('Stopping heartbeat')
        self.is_running = False
        if self.heartbeater:
            self.heartbeater.stop()
        self.socket.stop()
        self.heartbeat_thread.join()
