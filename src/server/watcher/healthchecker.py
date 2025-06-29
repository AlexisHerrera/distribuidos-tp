import logging
import subprocess
from threading import Lock

from src.common.cancellable_sleep import CancellableSleep
from src.common.runnable import Runnable
from src.messaging.protocol.healthcheck import HealthcheckProtocol
from src.messaging.tcp_socket import SocketDisconnected, TCPSocket

logger = logging.getLogger(__name__)

MAX_MISSING_HEALTHCHECKS = 3


class Healthchecker:
    def __init__(
        self, node_name: str, port: int, timeout: int, reconnection_timeout: int
    ):
        self.node_name = node_name
        self.port = port
        self.timeout = timeout
        self.reconnection_timeout = reconnection_timeout

        self.sleep = CancellableSleep()

        self.is_running = Runnable()

        self.socket_lock: Lock = Lock()
        self.socket: TCPSocket = None

    def run(self):
        healthcheck = 0

        try:
            self._first_connection()

            while self.is_running():
                try:
                    logger.debug(
                        f'[HEALTHCHECKER] Sending healthcheck to {self.node_name}'
                    )
                    self.socket.send(
                        HealthcheckProtocol.PING,
                        HealthcheckProtocol.MESSAGE_BYTES_AMOUNT,
                    )

                    logger.debug(
                        f'[HEALTHCHECKER] Waiting healthcheck of {self.node_name}'
                    )
                    message = self.socket.recv(HealthcheckProtocol.MESSAGE_BYTES_AMOUNT)

                    if message == HealthcheckProtocol.PONG:
                        logger.debug(
                            f'[HEALTHCHECKER] Received healthcheck from {self.node_name}. Sleeping...'
                        )
                        healthcheck = 0
                        self.sleep(self.timeout)
                except TimeoutError:
                    healthcheck += 1
                    logger.debug(
                        f'[HEALTHCHECKER] {self.node_name} has {healthcheck} unreplied healthcheck'
                    )
                except SocketDisconnected:
                    healthcheck = MAX_MISSING_HEALTHCHECKS
                    logger.warning(
                        f'[HEALTHCHECKER] {self.node_name} socket disconnected'
                    )
                except ConnectionError as e:
                    healthcheck = MAX_MISSING_HEALTHCHECKS
                    logger.warning(
                        f'[HEALTHCHECKER] {self.node_name} connection error: {e}'
                    )

                if healthcheck >= MAX_MISSING_HEALTHCHECKS:
                    self._restart_and_reconnect_service()
                    healthcheck = 0

        except Exception as e:
            logger.error(f'[HEALTHCHECKER] Error while watching {self.node_name}: {e}')

    def _first_connection(self):
        try:
            # Try to connect when node has just started
            self._connect_to_service()
            logger.debug(f'[HEALTHCHECKER] Connected to node {self.node_name}')
        except Exception:
            # Suppose that the node had some error to start
            # so force the start and connect
            self._restart_and_reconnect_service()

    def _restart_and_reconnect_service(self):
        while self.is_running():
            try:
                self._restart_service()
                self._connect_to_service()
                logger.debug(
                    f'[HEALTHCHECKER] Successfuly restarted and connected to {self.node_name}'
                )
                break
            except Exception as e:
                logger.warning(
                    f'[HEALTHCHECKER] Could not restart and reconnect to {self.node_name}: {e}'
                )
            self.sleep(self.reconnection_timeout)

    def _restart_service(self):
        logger.info(f'[HEALTHCHECKER] Restarting service {self.node_name}')
        result = subprocess.run(
            ['docker', 'start', self.node_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info(
            f'[HEALTHCHECKER] Command executed. Result={result.returncode}.'
            f'Output={result.stdout}. Error={result.stderr}'
        )

    def _connect_to_service(self):
        with self.socket_lock:
            if self.socket:
                self.socket.stop()

            addr = (self.node_name, self.port)
            self.socket = TCPSocket.create_and_connect(addr, self.timeout)

    def stop(self):
        self.is_running.stop()

        with self.socket_lock:
            if self.socket:
                self.socket.stop()

        self.sleep.cancel()
