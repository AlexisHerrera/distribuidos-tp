import logging
import threading

from src.messaging.server_socket import ServerSocket
from src.utils.config import Config

RECV_BYTES_AMOUNT = 1

logger = logging.getLogger(__name__)


class Heartbeat:
    def __init__(self, config: Config):
        self.socket = ServerSocket(config.heartbeat_port)
        self.is_running = True
        self.client = None
        self.heartbeat_thread = threading.Thread(target=self.run)
        self.heartbeat_thread.start()

    def _manage_client(self):
        message = b'B'  # Beat
        bytes_amount = len(message)
        while self.is_running:
            _ = self.client.recv(RECV_BYTES_AMOUNT)

            self.client.send(message, bytes_amount)

    def run(self):
        try:
            while self.is_running:
                self.client, _address = self.socket.accept()

                self._manage_client()
        except Exception as e:
            logger.error(f'Heartbeat error: {e}')

    def stop(self):
        logger.info('Stopping heartbeat')
        self.is_running = False
        if self.client:
            self.client.stop()
        self.socket.stop()
        self.heartbeat_thread.join()
