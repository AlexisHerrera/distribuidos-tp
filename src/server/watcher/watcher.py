import logging
import signal
import threading
from queue import SimpleQueue
from threading import Event
from typing import Callable

from src.common.runnable import Runnable
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
        self.is_running: bool = Runnable()

        self.healthcheck: Healthcheck = Healthcheck(config.healthcheck_port)

        self.nodes: list[str] = config.nodes
        self.peers: list[str] = [peer_name for peer_name in config.peers.keys()]

        self.healthcheckers: dict[str, (Healthchecker, threading.Thread)] = {}
        self.healthchecker_port: int = config.healthchecker_port
        self.healthchecker_timeout: int = config.healthchecker_timeout
        self.healthchecker_reconnection_timeout = (
            config.healthchecker_reconnection_timeout
        )

        # self.healthchecker_threads: list[threading.Thread] = []

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

    def _initialize_healthchecker(self, node_name: str):
        h = Healthchecker(
            node_name,
            self.healthchecker_port,
            self.healthchecker_timeout,
            self.healthchecker_reconnection_timeout,
        )
        t = threading.Thread(target=h.run)
        self.healthcheckers[node_name] = (h, t)
        # self.healthchecker_threads.append(t)

        t.start()

    def _add_healthcheckers(self, nodes: list[str]):
        for node in nodes:
            if node not in self.healthcheckers:
                self._initialize_healthchecker(node)

    def _stop_healthchecker(self, node_name: str):
        (h, t) = self.healthcheckers[node_name]
        h.stop()
        t.join()

    def _remove_healthcheckers(self, nodes: list[str]):
        for node in nodes:
            if node in self.healthcheckers:
                self._stop_healthchecker(node)
                self.healthcheckers.pop(node, None)  # Silently drop

    def _run_as_leader(self, change_leader: Event):
        nodes = self.nodes + self.peers

        self._add_healthcheckers(nodes)
        # self._initialize_healthcheckers(nodes)

        change_leader.wait()
        change_leader.clear()
        # Stop healthcheckers
        # self._stop_healthcheckers()
        # Only remove nodes from server, don't stop the healthcheck of peers
        self._remove_healthcheckers(self.nodes)

    def _run_as_follower(self, change_leader: Event, callback: Callable[[], None]):
        self._add_healthcheckers(self.peers)
        # self._initialize_healthcheckers(self.peers)

        while self.is_running():
            callback()
            change_leader.wait()
            change_leader.clear()
            if self.bully.am_i_leader():
                # Stop healthcheckers
                # self._stop_healthcheckers()
                break

    def run(self):
        while self.is_running():
            try:
                exit_msg = self.exit_queue.get()
                if exit_msg == EXIT_QUEUE:
                    return

            except Exception as e:
                logger.error(f'{e}')
                break

    # def _stop_healthcheckers(self):
    #     # Stop healthcheckers
    #     for _, h in self.healthcheckers.items():
    #         h.stop()
    #
    #     # Stop threads
    #     for t in self.healthchecker_threads:
    #         t.join()

    def stop(self):
        logger.info('[WATCHER] Stopping')
        self.is_running.stop()

        self.bully.stop()

        for _, (h, t) in self.healthcheckers.items():
            h.stop()
            t.join()

        self.healthcheck.stop()
        logger.info('[WATCHER] Stopped')
