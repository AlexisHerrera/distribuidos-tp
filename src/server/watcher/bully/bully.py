import logging
import time
from queue import SimpleQueue
from threading import Event, Lock, Thread, Timer
from typing import Callable

from src.server.watcher.bully.node_connection_manager import NodeConnectionManager
from src.server.watcher.bully.protocol import BullyProtocol
from src.server.watcher.bully.state_manager import BullyState, BullyStateManager

logger = logging.getLogger(__name__)


EXIT_MESSAGE_HANDLER = b'Q'
INITIAL_WAITING = 10


class Bully:
    def __init__(
        self,
        port: int,
        peers: dict[str, int],
        node_id: int,
        connection_timeout: int,
        as_leader: Callable[[Event], None],
        as_follower: Callable[[Event], None],
    ):
        self.peers = peers

        self.leader_lock = Lock()

        if len(peers) > 0:
            self.leader = max(max(peers.values()), node_id)
        else:
            self.leader = node_id

        self.node_id = node_id
        self.change_leader: Event = Event()

        logger.info(f'[BULLY] Leader is node_id: {self.leader}')

        self.as_leader = as_leader
        self.as_follower = as_follower

        self.message_queue: SimpleQueue = SimpleQueue()
        self.all_peers_connected: Event = Event()

        self.nodes: NodeConnectionManager = NodeConnectionManager(
            port=port,
            message_queue=self.message_queue,
            node_id=self.node_id,
            peers=peers,
            timeout=connection_timeout,
            all_peers_connected=self.all_peers_connected,
            is_leader=self._is_leader,
        )

        self.is_running_lock = Lock()
        self.is_running = True

        self.state: BullyStateManager = BullyStateManager(
            BullyState.ELECTION, self.node_id
        )

        self.runner_thread = Thread(target=self.run)
        self.message_handler = Thread(target=self._manage_recv)
        self.runner_thread.start()
        self.message_handler.start()

    def am_i_leader(self) -> bool:
        return self._is_leader(self.node_id)

    def _set_leader(self, node_id: int):
        logger.info(f'[BULLY] Setting new leader {node_id}')
        with self.leader_lock:
            self.leader = node_id

        self.change_leader.set()

    def _is_leader(self, node_id: int) -> bool:
        with self.leader_lock:
            return self.leader == node_id

    def run(self):
        self.all_peers_connected.wait(INITIAL_WAITING)
        self._init_election()
        self.change_leader.wait()
        self.change_leader.clear()

        while self._is_running():
            if self.am_i_leader():
                logger.info('[BULLY] Beginning leader tasks...')
                self.as_leader(self.change_leader)
            else:
                logger.info('[BULLY] Beginning follower tasks...')
                self.as_follower(self.change_leader, self._send_alive)

    def _send_alive(self):
        with self.leader_lock:
            actual_leader = self.leader

        while self.state.is_in_state(BullyState.RUNNING) and self._is_leader(
            actual_leader
        ):
            try:
                self.nodes.send_by_id(actual_leader, BullyProtocol.ALIVE)
            except Exception as e:
                logger.warning(f'[BULLY] Could not send ALIVE to leader: {e}')
            time.sleep(2)

    def _send_coordinator(self):
        self.nodes.send_all(BullyProtocol.COORDINATOR)

        logger.debug(f'[BULLY] Sending COORDINATOR from {self.node_id}')

    def _reply_alive(self, node_name: str):
        self.nodes.send(node_name, BullyProtocol.REPLY_ALIVE)
        logger.debug(f'[BULLY] Sending REPLY_ALIVE to {node_name}')

    def _init_election(self):
        self.nodes.send_higher(BullyProtocol.ELECTION)

        logger.info(f'[BULLY] Sending ELECTION from {self.node_id}')
        kwargs = {
            'is_leader': False,
            'init_election': self._init_election,
            'set_leader': self._set_leader,
            'send_coordinator': self._send_coordinator,
        }
        t = Timer(10.0, self.state.timeout_reply, kwargs=kwargs)
        t.start()

    def _send_answer(self, node_name: str):
        self.nodes.send(node_name, BullyProtocol.ANSWER)

        logger.debug(f'[BULLY] Sending ANSWER to {node_name}')

    def _manage_recv(self):
        logger.info('[BULLY] Begin manage recv')

        while self._is_running():
            (message, node_id, node_name) = self.message_queue.get()
            logger.debug(f'[BULLY] Received message {message} from {node_id}')

            match message:
                case BullyProtocol.ALIVE:
                    self._reply_alive(node_name)
                case BullyProtocol.ELECTION:
                    # Begin election
                    self.state.election(self._init_election)

                    if node_id < self.node_id:
                        self._send_answer(node_name)
                case BullyProtocol.ANSWER:
                    # Wait for coordinator message
                    self.state.answer()
                case BullyProtocol.COORDINATOR:
                    # Set new leader
                    self.state.coordinator(
                        new_leader_node_id=node_id,
                        set_leader=self._set_leader,
                        send_coordinator=self._send_coordinator,
                    )
                case BullyProtocol.REPLY_ALIVE:
                    # Do nothing
                    pass
                case BullyProtocol.TIMEOUT_REPLY:
                    self.state.timeout_reply(
                        is_leader=self._is_leader(node_id),
                        init_election=self._init_election,
                        set_leader=self._set_leader,
                        send_coordinator=self._send_coordinator,
                    )
                case _:  # Exit loop
                    break

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        self.state.set_state(BullyState.END)

        # Unlock threads that are waiting for leader change
        self.change_leader.set()
        # Unlock message handler thread
        self.message_queue.put((EXIT_MESSAGE_HANDLER, self.node_id, ''))

        self.nodes.stop()

        # Stop main threads
        self.runner_thread.join()
        self.message_handler.join()
