import logging
import threading
from queue import Empty, SimpleQueue

from src.messaging.protocol.heartbeat import HeartbeatProtocol
from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket

logger = logging.getLogger(__name__)


class Heartbeat:
    def __init__(self, heartbeat_port: int):
        self.socket = ServerSocket(heartbeat_port, 5)
        self.is_running_lock = threading.Lock()
        self.is_running = True
        self.manager_queue: SimpleQueue = SimpleQueue()
        self.clients: dict[tuple, (threading.Thread, TCPSocket)] = {}
        logger.info('[HEARTBEAT] Initiated heartbeat')
        self.heartbeat_thread = threading.Thread(target=self.run)
        self.heartbeat_thread.start()

    def _manage_client(self, socket: TCPSocket, addr: tuple):
        try:
            while self._is_running():
                message = socket.recv(HeartbeatProtocol.MESSAGE_BYTES_AMOUNT)
                logger.debug(f'[HEARTBEAT] Received {message} from {addr}')

                socket.send(
                    HeartbeatProtocol.PONG, HeartbeatProtocol.MESSAGE_BYTES_AMOUNT
                )
                logger.debug(f'[HEARTBEAT] Sent {HeartbeatProtocol.PONG} to {addr}')
        except Exception as e:
            logger.error(f'[HEARTBEAT] Error while managing client in heartbeat: {e}')
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
                heartbeater_socket, addr = self.socket.accept()
                logger.info(
                    f'[HEARTBEAT] Received new heartbeater from {TCPSocket.gethostbyaddress(addr)}'
                )

                client = threading.Thread(
                    target=self._manage_client, args=(heartbeater_socket, addr)
                )

                self.clients[addr] = (client, heartbeater_socket)

                client.start()

                self._clean_clients()
        except Exception as e:
            logger.error(f'[HEARTBEAT] Heartbeat error: {e}')

    def _is_running(self):
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        logger.info('[HEARTBEAT] Stopping heartbeat')
        with self.is_running_lock:
            self.is_running = False

        for _, (client, socket) in self.clients.items():
            socket.stop()
            client.join()

        self.socket.stop()
        self.heartbeat_thread.join()
