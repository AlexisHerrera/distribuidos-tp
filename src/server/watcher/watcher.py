import logging
import signal
import threading
from queue import SimpleQueue
from threading import Event

from src.server.heartbeat import Heartbeat
from src.server.watcher.bully import Bully
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
        self.heartbeat: Heartbeat = Heartbeat(config.heartbeat_port)
        self.bully: Bully = Bully(
            port=config.bully_port,
            peers=config.peers,
            node_id=config.node_id,
            as_leader=self._run_as_leader,
            as_follower=self._run_as_follower,
        )
        self.heartbeat_port: int = config.heartbeat_port
        self.heartbeaters: dict[str, Heartbeater] = {}
        self.nodes: list[str] = config.nodes
        self.peers: list[str] = [peer_name for peer_name in config.peers.keys()]

        self.heartbeater_threads: list[threading.Thread] = []

        self.exit_queue: SimpleQueue = SimpleQueue()
        self.timeout: int = config.timeout

        self.is_running_lock: threading.Lock = threading.Lock()
        self.is_running: bool = True
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

    def _initialize_heartbeaters(self):
        for node_name in self.nodes:
            h = Heartbeater(node_name, self.heartbeat_port, self.timeout)
            t = threading.Thread(target=h.run)
            self.heartbeaters[node_name] = h
            self.heartbeater_threads.append(t)

            t.start()

    def _run_as_leader(self, change_leader: Event):
        self._initialize_heartbeaters()

        change_leader.wait()
        # Stop heartbeaters

    def _run_as_follower(self, change_leader: Event):
        # self._initialize_heartbeaters() # for peers
        while self._is_running():
            change_leader.wait()
            if self.bully.am_i_leader():
                # Stop heartbeaters
                break

    def run(self):
        # self.bully.run()
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

        self.bully.stop()

        # Stop heartbeaters
        for _, h in self.heartbeaters.items():
            h.stop()

        # Stop threads
        for t in self.heartbeater_threads:
            t.join()

        # self.bully.stop()
        self.heartbeat.stop()
