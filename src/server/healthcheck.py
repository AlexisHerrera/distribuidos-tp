import logging
import threading
from queue import Empty, SimpleQueue

from src.messaging.protocol.healthcheck import HealthcheckerProtocol
from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket

logger = logging.getLogger(__name__)


class Healthcheck:
    def __init__(self, healthcheck_port: int):
        self.socket = ServerSocket(healthcheck_port, 5)
        self.is_running_lock = threading.Lock()
        self.is_running = True
        self.manager_queue: SimpleQueue = SimpleQueue()
        self.clients: dict[tuple, (threading.Thread, TCPSocket)] = {}
        logger.info('[HEALTHCHECK] Initiated healthcheck')
        self.healthcheck_thread = threading.Thread(target=self.run)
        self.healthcheck_thread.start()

    def _manage_client(self, socket: TCPSocket, addr: tuple):
        try:
            while self._is_running():
                message = socket.recv(HealthcheckerProtocol.MESSAGE_BYTES_AMOUNT)
                logger.debug(f'[HEALTHCHECK] Received {message} from {addr}')

                socket.send(
                    HealthcheckerProtocol.PONG,
                    HealthcheckerProtocol.MESSAGE_BYTES_AMOUNT,
                )
                logger.debug(
                    f'[HEALTHCHECK] Sent {HealthcheckerProtocol.PONG} to {addr}'
                )
        except Exception as e:
            logger.error(
                f'[HEALTHCHECK] Error while managing client in healthcheck: {e}'
            )
        finally:
            self.manager_queue.put(addr)

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
            while self._is_running():
                healthchecker_socket, addr = self.socket.accept()
                logger.info(
                    f'[HEALTHCHECK] Received new healthchecker from {TCPSocket.gethostbyaddress(addr)}'
                )

                client = threading.Thread(
                    target=self._manage_client, args=(healthchecker_socket, addr)
                )

                self.clients[addr] = (client, healthchecker_socket)

                client.start()

                self._clean_clients()
        except Exception as e:
            logger.error(f'[HEALTHCHECK] Healthcheck error: {e}')

    def _is_running(self):
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        logger.info('[HEALTHCHECK] Stopping healthcheck')
        with self.is_running_lock:
            self.is_running = False

        for _, (client, socket) in self.clients.items():
            socket.stop()
            client.join()

        self.socket.stop()
        self.healthcheck_thread.join()
