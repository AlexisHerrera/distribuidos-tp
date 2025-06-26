import logging
from queue import SimpleQueue
from threading import Thread
from typing import Callable

from src.common.runnable import Runnable
from src.messaging.protocol.bully import BullyProtocol
from src.messaging.tcp_socket import SocketDisconnected, TCPSocket

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
        reconnect: Callable[[str], TCPSocket],
        is_leader: Callable[[int], bool],
    ):
        self.is_running = Runnable()

        self.socket = socket
        self.node_name = node_name
        self.node_id = node_id
        self.message_queue = message_queue
        self.bully_node_id = bully_node_id
        self.reconnect = reconnect
        self.is_leader = is_leader
        self.recv_thread = Thread(target=self._manage_recv)
        self.recv_thread.start()

    def _manage_recv(self):
        logger.info(f'[BULLY] Begin recv of node id {self.node_id}')

        times = 0
        # Don't try to reconnect if socket comes from accept
        # Let the other node make the reconnection
        reconnect = True if self.socket is None else False
        try:
            while self.is_running():
                try:
                    if reconnect:
                        self.socket = self.reconnect(self.node_name)
                        reconnect = False

                    message = self.socket.recv(BullyProtocol.MESSAGE_BYTES_AMOUNT)

                    self.message_queue.put((message, self.node_id, self.node_name))
                    times = 0
                except TimeoutError as e:
                    if self.is_leader(self.node_id):
                        logger.warning(
                            f'[BULLY] Got timeout error from {self.node_name}: {e}'
                        )

                    times += 1
                except SocketDisconnected as e:
                    logger.warning(
                        f'[BULLY] Got socket disconnected error from {self.node_name}: {e}'
                    )
                    times = MAX_MISSED_MESSAGE
                    reconnect = True
                except ConnectionError as e:
                    logger.warning(
                        f'[BULLY] Got connection error from {self.node_name}: {e}'
                    )
                    times = MAX_MISSED_MESSAGE
                    reconnect = True

                if times >= MAX_MISSED_MESSAGE and self.is_leader(self.node_id):
                    self.message_queue.put(
                        (BullyProtocol.TIMEOUT_REPLY, self.node_id, self.node_name)
                    )
                    times = 0

        except Exception as e:
            logger.error(
                f'[BULLY] Error ocurred while recv message from {self.node_name} ID {self.node_id}: {e}'
            )

    def send(self, message: BullyProtocol):
        try:
            self.socket.send(message, BullyProtocol.MESSAGE_BYTES_AMOUNT)
        except Exception as e:
            logger.error(
                f'[BULLY] Error ocurred while sending message to {self.node_name} ID {self.node_id}: {e}'
            )

    def stop(self):
        logger.info(f'[BULLY] Stopping node {self.node_name}')
        self.is_running.stop()

        if self.socket:
            self.socket.stop()

        self.recv_thread.join()
        logger.info(f'[BULLY] Stopped node {self.node_name}')
