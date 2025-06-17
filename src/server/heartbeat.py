import logging
import threading

from src.messaging.udp_socket import UDPSocket
from src.utils.config import Config

RECV_BYTES_AMOUNT = 1

logger = logging.getLogger(__name__)


class Heartbeat:
    def __init__(self, config: Config):
        self.socket = UDPSocket(config.heartbeat_port)
        self.heartbeat_watcher_port = config.heartbeat_watcher_port
        self.is_running = True
        self.heartbeat_thread = threading.Thread(target=self.run)
        self.heartbeat_thread.start()

    def run(self):
        message = b'B'  # Beat
        bytes_amount = len(message)
        try:
            while self.is_running:
                # _message, (host, _port) = self.socket.recvfrom(RECV_BYTES_AMOUNT)
                recv_message, addr = self.socket.recvfrom(RECV_BYTES_AMOUNT)
                if recv_message is None:
                    break
                # logger.info(f'{host} {_port}')
                self.socket.sendto(
                    # (host, self.heartbeat_watcher_port),
                    addr,
                    message,
                    bytes_amount,
                )
        except Exception as e:
            logger.error(f'Heartbeat error: {e}')

    def stop(self):
        logger.info('Stopping heartbeat')
        self.is_running = False
        self.socket.stop()
        self.heartbeat_thread.join()
