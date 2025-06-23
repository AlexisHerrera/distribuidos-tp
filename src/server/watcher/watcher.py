import logging
import signal
import threading
from queue import SimpleQueue
from threading import Event
from typing import Callable

from src.server.heartbeat import Heartbeat
from src.server.watcher.bully.bully import Bully
from src.server.watcher.heartbeater import Heartbeater
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
        self.is_running_lock: threading.Lock = threading.Lock()
        self.is_running: bool = True

        self.heartbeat: Heartbeat = Heartbeat(config.heartbeat_port)

        self.heartbeat_port: int = config.heartbeat_port
        self.heartbeaters: dict[str, Heartbeater] = {}
        self.nodes: list[str] = config.nodes
        self.peers: list[str] = [peer_name for peer_name in config.peers.keys()]

        self.heartbeater_threads: list[threading.Thread] = []

        self.timeout: int = config.timeout
        self.reconnection_timeout = config.reconnection_timeout

        self.bully: Bully = Bully(
            port=config.bully_port,
            peers=config.peers,
            node_id=config.node_id,
            connection_timeout=config.timeout,
            as_leader=self._run_as_leader,
            as_follower=self._run_as_follower,
        )

        self.exit_queue: SimpleQueue = SimpleQueue()
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

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def _initialize_heartbeaters(self, nodes: list[str]):
        for node_name in nodes:
            h = Heartbeater(
                node_name, self.heartbeat_port, self.timeout, self.reconnection_timeout
            )
            t = threading.Thread(target=h.run)
            self.heartbeaters[node_name] = h
            self.heartbeater_threads.append(t)

            t.start()

    def _run_as_leader(self, change_leader: Event):
        self._initialize_heartbeaters(self.nodes)

        change_leader.wait()
        change_leader.clear()
        # Stop heartbeaters
        self._stop_heartbeaters()

    def _run_as_follower(self, change_leader: Event, callback: Callable[[], None]):
        self._initialize_heartbeaters(self.peers)

        while self._is_running():
            callback()
            change_leader.wait()
            change_leader.clear()
            if self.bully.am_i_leader():
                # Stop heartbeaters
                self._stop_heartbeaters()
                break

    def run(self):
        while self._is_running():
            try:
                exit_msg = self.exit_queue.get()
                if exit_msg == EXIT_QUEUE:
                    return

            except Exception as e:
                logger.error(f'{e}')
                break

    def _stop_heartbeaters(self):
        # Stop heartbeaters
        for _, h in self.heartbeaters.items():
            h.stop()

        # Stop threads
        for t in self.heartbeater_threads:
            t.join()

    def stop(self):
        logger.info('[WATCHER] Stopping')
        with self.is_running_lock:
            self.is_running = False

        self.bully.stop()

        self.heartbeat.stop()
        logger.info('[WATCHER] Stopped')
