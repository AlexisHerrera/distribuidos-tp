import logging
import signal
import threading
from queue import SimpleQueue
from threading import Event
from typing import Callable

from src.server.healthcheck import Healthcheck
from src.server.watcher.bully.bully import Bully
from src.server.watcher.healthchecker import Healthchecker
from src.utils.config import WatcherConfig

logger = logging.getLogger(__name__)

MAX_MISSING_HEALTHCHECKS = 3
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

        self.healthcheck: Healthcheck = Healthcheck(config.healthcheck_port)

        self.healthchecker_port: int = config.healthchecker_port
        self.healthcheckers: dict[str, Healthchecker] = {}
        self.nodes: list[str] = config.nodes
        self.peers: list[str] = [peer_name for peer_name in config.peers.keys()]

        self.healthchecker_threads: list[threading.Thread] = []

        self.healthchecker_timeout: int = config.healthchecker_port
        self.healthchecker_reconnection_timeout = (
            config.healthchecker_reconnection_timeout
        )

        self.bully: Bully = Bully(
            port=config.bully_port,
            peers=config.peers,
            node_id=config.node_id,
            connection_timeout=config.bully_timeout,
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

    def _initialize_healthcheckers(self, nodes: list[str]):
        for node_name in nodes:
            h = Healthchecker(
                node_name,
                self.healthchecker_port,
                self.healthchecker_timeout,
                self.healthchecker_reconnection_timeout,
            )
            t = threading.Thread(target=h.run)
            self.healthcheckers[node_name] = h
            self.healthchecker_threads.append(t)

            t.start()

    def _run_as_leader(self, change_leader: Event):
        self._initialize_healthcheckers(self.nodes)

        change_leader.wait()
        change_leader.clear()
        # Stop healthcheckers
        self._stop_healthcheckers()

    def _run_as_follower(self, change_leader: Event, callback: Callable[[], None]):
        self._initialize_healthcheckers(self.peers)

        while self._is_running():
            callback()
            change_leader.wait()
            change_leader.clear()
            if self.bully.am_i_leader():
                # Stop healthcheckers
                self._stop_healthcheckers()
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

    def _stop_healthcheckers(self):
        # Stop healthcheckers
        for _, h in self.healthcheckers.items():
            h.stop()

        # Stop threads
        for t in self.healthchecker_threads:
            t.join()

    def stop(self):
        logger.info('[WATCHER] Stopping')
        with self.is_running_lock:
            self.is_running = False

        self.bully.stop()

        self.healthcheck.stop()
        logger.info('[WATCHER] Stopped')
