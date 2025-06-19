import logging
import signal
import subprocess
import threading
import time
from queue import Queue, SimpleQueue

from src.messaging.tcp_socket import TCPSocket

logger = logging.getLogger(__name__)

MAX_MISSING_HEARTBEATS = 3
MESSAGE_TO_SEND = b'H'  # Heart
SEND_BYTES_AMOUNT = len(MESSAGE_TO_SEND)
EXPECTED_REPLY_MESSAGE = b'B'  # Beat
RECV_BYTES_AMOUNT = len(EXPECTED_REPLY_MESSAGE)

EXIT_QUEUE = 'q'


# 2 ways:
# 1. Create a thread per node and manage every node separately
# pro: easier to manage
# con: too many threads?
# 2. Iterate over every node and send heartbeats, then wait for response
# pro: no need to create more threads
# con: heartbeats are tied between nodes
class Watcher:
    def __init__(self, heartbeat_port: int, nodes: list[str], timeout: int):
        self.heartbeat_port: int = heartbeat_port
        self.nodes: list[str] = nodes
        self.sockets: dict[str, TCPSocket] = {}

        self.heartbeater_threads: list[threading.Thread] = []

        self.main_queue: SimpleQueue = SimpleQueue()
        self.heartbeater_queues: dict[str, Queue] = {}
        self.timeout: int = timeout

        self.is_running = True
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        def signal_handler(signum, _frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.main_queue.put(EXIT_QUEUE)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Signal handlers configured.')

    def _restart_service(self, node_name: str):
        logger.info(f'Restarting service {node_name}')
        result = subprocess.run(
            ['docker', 'start', node_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info(
            f'Command executed. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}'
        )

    def _watch_node(
        self,
        node_name: str,
        socket: TCPSocket,
        watcher_queue: Queue,
        main_queue: SimpleQueue,
        timeout: int,
    ):
        heartbeats = 0

        try:
            while True:
                try:
                    logger.debug(f'Sending heartbeat to {node_name}')
                    socket.send(MESSAGE_TO_SEND, SEND_BYTES_AMOUNT)

                    logger.debug(f'Waiting heartbeat of {node_name}')
                    message = socket.recv(RECV_BYTES_AMOUNT)
                    if message is None:
                        raise ConnectionError
                    if message == EXPECTED_REPLY_MESSAGE:
                        logger.debug(
                            f'Received heartbeat from {node_name}. Sleeping...'
                        )
                        heartbeats = 0
                        time.sleep(timeout)
                except TimeoutError:
                    heartbeats += 1
                    logger.debug(f'{node_name} has {heartbeats} unreplied')
                except ConnectionError:
                    heartbeats = MAX_MISSING_HEARTBEATS

                if heartbeats >= MAX_MISSING_HEARTBEATS:
                    self._restart_service(node_name)
                    main_queue.put(node_name)
                    socket = watcher_queue.get()
                    watcher_queue.task_done()
                    heartbeats = 0
        except Exception as e:
            logger.error(f'Error while watching node: {e}')

    def _connect_to_node(self, addr: tuple, timeout: int) -> TCPSocket:
        while True:
            try:
                socket = TCPSocket.create_and_connect(addr, timeout)
                return socket
            except ConnectionRefusedError:
                logger.warning(f'Could not connect to {addr}')
                time.sleep(timeout)

    def _initialize_heartbeaters(self):
        for node_name in self.nodes:
            client_queue = Queue()
            self.heartbeater_queues[node_name] = client_queue

            socket = self._connect_to_node(
                (node_name, self.heartbeat_port), self.timeout
            )
            self.sockets[node_name] = socket

            t = threading.Thread(
                target=self._watch_node,
                args=(
                    node_name,
                    socket,
                    client_queue,
                    self.main_queue,
                    self.timeout,
                ),
            )

            t.start()

            self.heartbeater_threads.append(t)

    def run(self):
        self._initialize_heartbeaters()

        while self.is_running:
            try:
                node_name = self.main_queue.get()
                if node_name == EXIT_QUEUE:
                    return

                old_socket = self.sockets[node_name]
                old_socket.stop()

                socket = self._connect_to_node(
                    addr=(node_name, self.heartbeat_port), timeout=self.timeout
                )
                queue = self.heartbeater_queues[node_name]
                queue.put(socket, block=False)

                self.sockets[node_name] = socket
            except Exception as e:
                logger.error(f'{e}')
                break

    def stop(self):
        self.is_running = False

        # Stop sockets
        for _, s in self.sockets.items():
            s.stop()

        # Stop threads
        for h in self.heartbeater_threads:
            h.join()
