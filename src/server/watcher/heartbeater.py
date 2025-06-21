import logging
import subprocess
import time
from threading import Lock

from src.messaging.tcp_socket import TCPSocket

logger = logging.getLogger(__name__)

MAX_MISSING_HEARTBEATS = 3
MESSAGE_TO_SEND = b'H'  # Heart
SEND_BYTES_AMOUNT = len(MESSAGE_TO_SEND)
EXPECTED_REPLY_MESSAGE = b'B'  # Beat
RECV_BYTES_AMOUNT = len(EXPECTED_REPLY_MESSAGE)


class Heartbeater:
    def __init__(self, node_name: str, port: int, timeout: int):
        self.node_name = node_name
        self.port = port
        self.timeout = timeout

        self.is_running_lock: Lock = Lock()
        self.is_running = True

        self.socket_lock: Lock = Lock()
        self.socket: TCPSocket = None

    def run(self):
        heartbeats = 0

        try:
            self._get_socket()

            while self._is_running():
                try:
                    logger.debug(f'Sending heartbeat to {self.node_name}')
                    self.socket.send(MESSAGE_TO_SEND, SEND_BYTES_AMOUNT)

                    logger.debug(f'Waiting heartbeat of {self.node_name}')
                    message = self.socket.recv(RECV_BYTES_AMOUNT)

                    if message is None:
                        raise ConnectionError

                    if message == EXPECTED_REPLY_MESSAGE:
                        logger.debug(
                            f'Received heartbeat from {self.node_name}. Sleeping...'
                        )
                        heartbeats = 0
                        time.sleep(self.timeout)
                except TimeoutError:
                    heartbeats += 1
                    logger.debug(
                        f'{self.node_name} has {heartbeats} unreplied heartbeats'
                    )
                except ConnectionError:
                    heartbeats = MAX_MISSING_HEARTBEATS
                    logger.warning(f'{self.node_name} with connection error')

                if heartbeats >= MAX_MISSING_HEARTBEATS:
                    self._restart_service()
                    self._get_socket()
                    heartbeats = 0

        except Exception as e:
            logger.error(f'Error while watching {self.node_name}: {e}')

    def _restart_service(self):
        logger.info(f'Restarting service {self.node_name}')
        result = subprocess.run(
            ['docker', 'start', self.node_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info(
            f'Command executed. Result={result.returncode}.'
            f'Output={result.stdout}. Error={result.stderr}'
        )

    def _get_socket(self):
        with self.socket_lock:
            if self.socket:
                self.socket.stop()

            self.socket = TCPSocket.connect_while_condition(
                host=self.node_name,
                port=self.port,
                condition=self._is_running,
                timeout=self.timeout,
            )

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        with self.socket_lock:
            if self.socket:
                self.socket.stop()
