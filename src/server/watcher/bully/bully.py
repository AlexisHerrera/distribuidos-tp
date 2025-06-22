import logging
import time
from queue import SimpleQueue
from threading import Event, Lock, Thread, Timer
from typing import Callable

from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket
from src.server.watcher.bully.node import BullyNode
from src.server.watcher.bully.protocol import BullyProtocol
from src.server.watcher.bully.state_manager import BullyState, BullyStateManager

logger = logging.getLogger(__name__)


EXIT_MESSAGE_HANDLER = b'Q'


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
        self.bully_port = port
        self.timeout = connection_timeout
        self.socket: ServerSocket = ServerSocket(
            self.bully_port, backlog=len(peers) + 1
        )

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

        self.nodes_lock: Lock = Lock()
        self.nodes: dict[str, BullyNode] = {}

        self.is_running_lock = Lock()
        self.is_running = True

        # initial_state = (
        #     BullyState.RUNNING if self.am_i_leader() else BullyState.WAITING_COORDINATOR
        # )
        initial_state = BullyState.RUNNING
        self.state: BullyStateManager = BullyStateManager(initial_state, self.node_id)

        self.message_queue: SimpleQueue = SimpleQueue()
        self.runner_thread = Thread(target=self.run)
        self.message_handler = Thread(target=self._manage_recv)
        self.listener_thread = Thread(target=self._listener)
        self.runner_thread.start()
        self.message_handler.start()
        self.listener_thread.start()

    def am_i_leader(self) -> bool:
        with self.leader_lock:
            return self.leader == self.node_id

    def _set_leader(self, node_id: int):
        logger.info(f'[BULLY] Setting new leader {node_id}')
        with self.leader_lock:
            self.leader = node_id

        self.change_leader.set()

    def _get_leader_node_id(self) -> int:
        with self.leader_lock:
            return self.leader

    def _add_node(self, socket: TCPSocket, node_name: str, node_id: int):
        with self.nodes_lock:
            if node_name in self.nodes:
                self.nodes[node_name].stop()

            node = BullyNode(
                socket,
                node_name,
                node_id,
                self.message_queue,
                self.node_id,
                self._get_leader_node_id,
            )

            self.nodes[node_name] = node
            logger.debug(f'[BULLY] Nodes {self.nodes.keys()}')

        logger.info(f'[BULLY] Added node {node_name} {node_id}')

    def _get_node_id_by_name(self, node_name: str) -> int:
        return self.peers[node_name]

    def _get_node_name_by_id(self, node_id: int) -> str:
        for node_name, v in self.peers.items():
            if v == node_id:
                return node_name

    def _listener(self):
        try:
            self._connect_to_peers()

            while self._is_running():
                try:
                    client_socket, addr = self.socket.accept()

                    node_name = TCPSocket.gethostbyaddress(addr)
                    node_id = self._get_node_id_by_name(node_name)

                    logger.debug(
                        f'[BULLY] Node {node_id} ({node_name}) connected to node {self.node_id}'
                    )

                    self._add_node(client_socket, node_name, node_id)
                except ConnectionAbortedError as e:
                    logger.error(f'[BULLY] Error while accepting client: {e}')
                except TimeoutError:
                    continue
        except Exception as e:
            logger.error(
                f'[BULLY] Error while listening for connections in node_id {self.node_id}: {e}'
            )

    def _connect_to_peers(self):
        if len(self.peers) == 0:
            return

        sorted_peers = sorted(
            [(name, nid) for name, nid in self.peers.items()],
            key=lambda node: node[1],
        )

        # rotated_peers = rotate(sorted_peers, self.node_id - 1)

        for node_name, node_id in sorted_peers:
            # Check if node has already connected
            if self.node_id > node_id:
                continue

            socket = TCPSocket.connect_while_condition(
                node_name,
                self.bully_port,
                condition=self._is_running,
                timeout=self.timeout,
            )

            logger.debug(f'[BULLY] Connected to {node_name}')

            self._add_node(socket, node_name, node_id)

    def run(self):
        # self._connect_to_peers()

        while self._is_running():
            if self.am_i_leader():
                logger.info('[BULLY] Beginning leader tasks...')
                # self._send_coordinator()
                self.as_leader(self.change_leader)
            else:
                logger.info('[BULLY] Beginning follower tasks...')
                self.as_follower(self.change_leader, self._send_alive)

    def _send_alive(self):
        while self.state.is_in_state(BullyState.RUNNING):
            try:
                with self.leader_lock:
                    node_name = self._get_node_name_by_id(self.leader)
                    with self.nodes_lock:
                        self.nodes[node_name].send(BullyProtocol.ALIVE)
            except Exception as e:
                logger.warning(f'[BULLY] Could not send ALIVE to leader: {e}')
            time.sleep(2)

    def _send_coordinator(self):
        with self.nodes_lock:
            for _, node in self.nodes.items():
                node.send(BullyProtocol.COORDINATOR)

        logger.debug(f'[BULLY] Sending COORDINATOR from {self.node_id}')

    def _reply_alive(self, node_name: str):
        with self.nodes_lock:
            self.nodes[node_name].send(BullyProtocol.REPLY_ALIVE)

        logger.debug(f'[BULLY] Sending REPLY_ALIVE to {node_name}')

    def _init_election(self):
        with self.nodes_lock:
            for _, node in self.nodes.items():
                if node.node_id > self.node_id:
                    node.send(BullyProtocol.ELECTION)

        logger.info(f'[BULLY] Sending ELECTION from {self.node_id}')
        kwargs = {
            'init_election': self._init_election,
            'set_leader': self._set_leader,
            'send_coordinator': self._send_coordinator,
        }
        t = Timer(10.0, self.state.timeout_reply, kwargs=kwargs)
        t.start()

    def _send_answer(self, node_name: str):
        with self.nodes_lock:
            self.nodes[node_name].send(BullyProtocol.ANSWER)

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
                    )
                case BullyProtocol.REPLY_ALIVE:
                    # Do nothing
                    pass
                case BullyProtocol.TIMEOUT_REPLY:
                    self.state.timeout_reply(
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

        self.socket.stop()
        for _, n in self.nodes.items():
            n.stop()

        # Stop main threads
        self.listener_thread.join()
        self.runner_thread.join()
        self.message_handler.join()
