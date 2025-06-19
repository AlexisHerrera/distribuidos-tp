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
MAX_CONNECT_TRIES = 3


class Watcher:
    def __init__(self, heartbeat_port: int, nodes: list[str], timeout: int):
        self.heartbeat_port: int = heartbeat_port
        self.nodes: list[str] = nodes
        self.sockets_lock: threading.Lock = threading.Lock()
        self.sockets: dict[str, TCPSocket] = {}

        self.heartbeater_threads: list[threading.Thread] = []

        self.exit_queue: SimpleQueue = SimpleQueue()
        self.heartbeater_queues: dict[str, Queue] = {}
        self.timeout: int = timeout

        self.is_running = True
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        def signal_handler(signum, _frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.exit_queue.put(EXIT_QUEUE)

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
        heartbeat_port: int,
        timeout: int,
    ):
        heartbeats = 0
        socket = self._connect_to_node(
            node_name=node_name, port=heartbeat_port, timeout=timeout
        )

        try:
            while self.is_running:
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
                    logger.debug(f'{node_name} has {heartbeats} unreplied heartbeats')
                except ConnectionError:
                    heartbeats = MAX_MISSING_HEARTBEATS
                    logger.warning(f'{node_name} with connection error')

                if heartbeats >= MAX_MISSING_HEARTBEATS:
                    self._restart_service(node_name)
                    socket = self._connect_to_node(
                        node_name=node_name, port=heartbeat_port, timeout=timeout
                    )
                    heartbeats = 0
        except Exception as e:
            logger.error(f'Error while watching {node_name}: {e}')

    def _connect_to_node(
        self, node_name: str, port: int, timeout: int
    ) -> TCPSocket | None:
        socket = None
        while self.is_running:
            try:
                socket = TCPSocket.create_and_connect(
                    addr=(node_name, port), timeout=timeout
                )
                break
            except ConnectionRefusedError as e:
                logger.warning(f'Could not connect to {node_name}:{port}: {e}')
            except OSError as e:
                logger.warning(f'Could not connect to {node_name}:{port}: {e}')

            time.sleep(timeout)

        if socket:
            with self.sockets_lock:
                old_socket = self.sockets.get(node_name, None)

                if old_socket:
                    old_socket.stop()

                self.sockets[node_name] = socket

        return socket

    def _initialize_heartbeaters(self):
        for node_name in self.nodes:
            t = threading.Thread(
                target=self._watch_node,
                args=(
                    node_name,
                    self.heartbeat_port,
                    self.timeout,
                ),
            )

            t.start()

            self.heartbeater_threads.append(t)

    def run(self):
        self._initialize_heartbeaters()

        while self.is_running:
            try:
                exit_msg = self.exit_queue.get()
                if exit_msg == EXIT_QUEUE:
                    return

            except Exception as e:
                logger.error(f'{e}')
                break

    def stop(self):
        self.is_running = False

        # Stop sockets
        with self.sockets_lock:
            for _, s in self.sockets.items():
                s.stop()

        # Stop threads
        for h in self.heartbeater_threads:
            h.join()
