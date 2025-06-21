import logging
import signal
import subprocess
import threading
import time
from queue import SimpleQueue

from src.messaging.tcp_socket import TCPSocket
from src.server.heartbeat import Heartbeat
from src.utils.config import WatcherConfig

logger = logging.getLogger(__name__)

MAX_MISSING_HEARTBEATS = 3
MESSAGE_TO_SEND = b'H'  # Heart
SEND_BYTES_AMOUNT = len(MESSAGE_TO_SEND)
EXPECTED_REPLY_MESSAGE = b'B'  # Beat
RECV_BYTES_AMOUNT = len(EXPECTED_REPLY_MESSAGE)

EXIT_QUEUE = 'q'
MAX_CONNECT_TRIES = 3


class Watcher:
    def __init__(self, config: WatcherConfig):
        self.heartbeat: Heartbeat = Heartbeat(config.heartbeat_port)
        # self.bully = Bully(config.bully_port, config.peers)
        self.heartbeat_port: int = config.heartbeat_port
        self.nodes: list[str] = config.nodes
        self.peers: list[str] = config.peers
        self.sockets_lock: threading.Lock = threading.Lock()
        self.sockets: dict[str, TCPSocket] = {}

        self.heartbeater_threads: list[threading.Thread] = []

        self.exit_queue: SimpleQueue = SimpleQueue()
        self.timeout: int = config.timeout

        self.is_running_lock = threading.Lock()
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

        try:
            socket = self._get_socket(node_name, heartbeat_port)

            while self._is_running():
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
                    socket = self._get_socket(node_name, heartbeat_port)
                    heartbeats = 0

        except Exception as e:
            logger.error(f'Error while watching {node_name}: {e}')

    def _update_sockets(self, node_name: str, socket: TCPSocket):
        if socket:
            with self.sockets_lock:
                old_socket = self.sockets.get(node_name, None)

                if old_socket:
                    old_socket.stop()

                self.sockets[node_name] = socket

    def _get_socket(self, node_name: str, port: int) -> TCPSocket:
        socket = TCPSocket.connect_while_condition(
            node_name,
            port,
            condition=self._is_running,
            timeout=self.timeout,
        )
        self._update_sockets(node_name, socket)

        return socket

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def _initialize_heartbeaters(self):
        # if self.bully.am_i_leader():
        #     for node_name in self.nodes...
        # else:
        #     self.heartbeats.stop()
        #     heartbeat to other watchers
        for node_name in self.nodes:
            # h = Heartbeater(node_name, self.heartbeat_port, self.timeout)
            # threading.Thread(target=h.run)
            # t.start()
            #
            # heartbeaters.stop()
            # t.join()
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

        while self._is_running():
            try:
                exit_msg = self.exit_queue.get()
                if exit_msg == EXIT_QUEUE:
                    return

            except Exception as e:
                logger.error(f'{e}')
                break

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        # Stop sockets
        with self.sockets_lock:
            for _, s in self.sockets.items():
                s.stop()

        # Stop threads
        for h in self.heartbeater_threads:
            h.join()

        # self.bully.stop()
        self.heartbeat.stop()
