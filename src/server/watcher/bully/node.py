import logging
import time
from queue import SimpleQueue
from threading import Lock, Thread

from src.messaging.tcp_socket import TCPSocket
from src.server.watcher.bully.protocol import MESSAGE_BYTES_AMOUNT, BullyProtocol

logger = logging.getLogger(__name__)

MAX_MISSED_MESSAGE = 3


class BullyNode:
    def __init__(
        self,
        socket: TCPSocket,
        node_name: str,
        node_id: int,
        message_queue: SimpleQueue,
        bully_node_id: int,
    ):
        self.is_running_lock = Lock()
        self.is_running = True
        self.socket = socket
        self.node_name = node_name
        self.node_id = node_id
        self.message_queue = message_queue
        self.bully_node_id = bully_node_id
        self.recv_thread = Thread(target=self._manage_recv)
        self.recv_thread.start()

    def _manage_recv(self):
        logger.info(f'[BULLY] Begin recv of node id {self.node_id}')

        times = 0
        try:
            while self._is_running():
                try:
                    message = self.socket.recv(MESSAGE_BYTES_AMOUNT)

                    self.message_queue.put((message, self.node_id, self.node_name))
                    times = 0
                except TimeoutError:
                    times += 1
                    if times > MAX_MISSED_MESSAGE and self.node_id > self.bully_node_id:
                        self.message_queue.put(
                            (BullyProtocol.TIMEOUT_REPLY, self.node_id, self.node_name)
                        )
                        times = 0
        except Exception as e:
            logger.error(
                f'[BULLY] Error ocurred while recv message from {self.node_name}: {e}'
            )

    def send(self, message: BullyProtocol):
        time.sleep(2)
        self.socket.send(message, MESSAGE_BYTES_AMOUNT)

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        if self.socket:
            self.socket.stop()

        self.recv_thread.join()
