import logging
import threading
import time
from queue import Empty, SimpleQueue

from src.messaging.udp_socket import UDPSocket

logger = logging.get_logger(__name__)

MAX_MISSING_HEARTBEATS = 3
RECV_BYTES_AMOUNT = 1


# 2 ways:
# 1. Create a thread per node and manage every node separately
# pro: easier to manage
# con: too many threads?
# 2. Iterate over every node and send heartbeats, then wait for response
# pro: no need to create more threads
# con: heartbeats are tied between nodes
class Watcher:
    def __init__(
        self, server_port: int, client_port: int, nodes: list[str], timeout: int
    ):
        # Create sockets
        self.server_socket = UDPSocket(server_port)
        self.client_socket_lock = threading.Lock()
        self.client_socket = UDPSocket(client_port)

        # Get nodes from config
        self.nodes: list[str] = nodes
        self.queues: dict[str, SimpleQueue] = {}

        self.timeout = timeout

    def restart_service(self, node_name: str):
        logger.info(f'Restarting service {node_name}')

    def _watch_node(self, queue: SimpleQueue, node_name: str, timeout: int):
        heartbeats = 0
        message = 'Ping'
        bytes_amount = len(message)
        try:
            while True:
                with self.client_socket_lock:
                    self.client_socket.sendto(node_name, message, bytes_amount)

                try:
                    message = queue.get(timeout=timeout)
                    if message:
                        heartbeats = 0
                        # Change to another timeout?
                        time.sleep(timeout)
                except Empty:
                    heartbeats += 1

                if heartbeats > MAX_MISSING_HEARTBEATS:
                    self.restart_service(node_name)
        except Exception as e:
            logger.error(f'{e}')

    def run(self):
        for node_name in self.nodes:
            queue = SimpleQueue()
            self.queues[node_name] = queue

            t = threading.Thread(
                target=self._watch_node,
                args=(
                    queue,
                    node_name,
                    self.timeout,
                ),
            )
            t.start()
            self.client_threads(t)

        try:
            while True:
                message, addr = self.server_socket.recvfrom(RECV_BYTES_AMOUNT)
                logger.info(f'Received {message} from {addr}')
                # queue = self.queues[addr]
                # queue.put(message)
        except Exception as e:
            logger.error(f'{e}')

    def stop(self):
        # Stop socket
        self.server_socket.stop()
        self.client_socket.stop()
        # Stop threads
