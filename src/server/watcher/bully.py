import logging
from enum import Enum
from queue import SimpleQueue
from threading import Event, Lock, Thread
from typing import Callable

from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket
from src.utils.util import rotate

logger = logging.getLogger(__name__)


class BullyProtocol:
    ALIVE = b'A'
    REPLY_ALIVE = b'R'
    ELECTION = b'E'
    ANSWER = b'W'
    COORDINATOR = b'C'
    TIMEOUT_REPLY = b'T'


class BullyState(Enum):
    RUNNING = (1,)
    WAITING_COORDINATOR = (2,)
    ELECTION = 3


MESSAGE_BYTES_AMOUNT = 1
EXIT_MESSAGE_HANDLER = b'Q'
DEFAULT_TIMEOUT = 1


class Bully:
    def __init__(
        self,
        port: int,
        peers: dict[str, int],
        node_id: int,
        as_leader: Callable[[Event], None],
        as_follower: Callable[[Event], None],
    ):
        self.bully_port = port
        self.socket: ServerSocket = ServerSocket(
            self.bully_port, backlog=len(peers) + 1, timeout=DEFAULT_TIMEOUT
        )

        self.peers = peers

        self.leader_lock = Lock()

        if len(peers) > 0:
            self.leader = max(max(peers.values()), node_id)
        else:
            self.leader = node_id

        self.node_id = node_id
        self.change_leader: Event = Event()
        logger.info(f'Leader is node_id: {self.leader}')
        self.as_leader = as_leader
        self.as_follower = as_follower

        self.nodes_lock: Lock = Lock()
        self.nodes: dict[str, BullyNode] = {}

        self.is_running_lock = Lock()
        self.is_running = True

        self.state: BullyState = BullyState.RUNNING

        self.message_queue: SimpleQueue = SimpleQueue()
        self.message_handler = Thread(target=self._manage_recv)
        self.listener_thread = Thread(target=self._listener)
        self.runner_thread = Thread(target=self.run)
        self.message_handler.start()
        self.listener_thread.start()
        self.runner_thread.start()

    def am_i_leader(self) -> bool:
        with self.leader_lock:
            return self.leader == self.node_id

    def _set_leader(self, node_id: int):
        logger.info(f'Setting new leader {node_id}')
        with self.leader_lock:
            self.leader = node_id

        self.change_leader.set()

    def _get_leader_id(self) -> int:
        with self.leader_lock:
            return self.leader

    def _add_node(self, socket: TCPSocket, node_name: str, node_id: int):
        with self.nodes_lock:
            if node_name in self.nodes:
                self.nodes[node_name].stop()

            node = BullyNode(
                socket, node_name, node_id, self.message_queue, self._get_leader_id
            )

            self.nodes[node_name] = node
            logger.debug(f'Nodes {self.nodes.keys()}')

        logger.info(f'Added node {node_name} {node_id}')

    def _get_node_id_by_name(self, node_name: str) -> int:
        return self.peers[node_name]

    def _listener(self):
        try:
            while self._is_running():
                try:
                    client_socket, addr = self.socket.accept()

                    node_name = TCPSocket.gethostbyaddress(addr)
                    node_id = self._get_node_id_by_name(node_name)

                    logger.debug(
                        f'Node {node_id} ({node_name}) connected to node {self.node_id}'
                    )

                    self._add_node(client_socket, node_name, node_id)
                except ConnectionAbortedError as e:
                    logger.error(f'Error while accepting client: {e}')
        except Exception as e:
            logger.error(
                f'Error while listening for connections in node_id {self.node_id}: {e}'
            )

    def _connect_to_peers(self):
        if len(self.peers) == 0:
            return

        sorted_peers = sorted(
            [(name, nid) for name, nid in self.peers.items()],
            key=lambda node: node[1],
        )

        rotated_peers = rotate(sorted_peers, self.node_id - 1)

        for node_name, node_id in rotated_peers:
            # Check if node has already connected
            with self.nodes_lock:
                if node_id in self.nodes:
                    continue

            socket = TCPSocket.connect_while_condition(
                node_name, self.bully_port, condition=self._is_running
            )

            logger.debug(f'Connected to {node_name}')

            self._add_node(socket, node_name, node_id)

    def run(self):
        self._connect_to_peers()

        while self._is_running():
            if self.am_i_leader():
                self._send_coordinator()
                self.as_leader(self.change_leader)
            else:
                self.as_follower(self.change_leader)

    def _send_coordinator(self):
        with self.nodes_lock:
            for _, node in self.nodes.items():
                node.send(BullyProtocol.COORDINATOR)

    def _reply_alive(self, node_id: int):
        with self.nodes_lock:
            self.nodes[node_id].send(BullyProtocol.REPLY_ALIVE)

    def _manage_recv(self):
        while self._is_running():
            (message, node_id) = self.message_queue.get()

            match message:
                case BullyProtocol.ALIVE:
                    self._reply_alive(node_id)
                case BullyProtocol.ELECTION:
                    # Begin election
                    if self.state != BullyState.ELECTION:
                        self.state = BullyState.ELECTION
                        with self.nodes_lock:
                            for _, node in self.nodes.items():
                                if node.node_id > self.node_id:
                                    node.send(BullyProtocol.ELECTION)
                                elif node.node_id == node_id:
                                    node.send(BullyProtocol.ANSWER)
                    else:
                        if node_id < self.node_id:
                            with self.nodes_lock:
                                self.nodes[node_id].send(BullyProtocol.ANSWER)
                case BullyProtocol.ANSWER:
                    # Wait for coordinator message
                    self.state = BullyState.WAITING_COORDINATOR
                case BullyProtocol.COORDINATOR:
                    # Set new leader
                    if (
                        self.state == BullyState.WAITING_COORDINATOR
                        and self.node_id < node_id
                    ):
                        self._set_leader(node_id)
                        self.state = BullyState.RUNNING
                    pass
                case BullyProtocol.REPLY_ALIVE:
                    pass
                case BullyProtocol.TIMEOUT_REPLY:
                    if self.state == BullyState.RUNNING:
                        self.state = BullyState.ELECTION
                        self._init_election()
                    elif self.state == BullyState.ELECTION:
                        self.state = BullyState.RUNNING
                        self._set_leader(self.node_id)
                        self._send_coordinator()
                case _:  # Exit loop
                    break

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        # Unlock threads that are waiting for leader change
        self.change_leader.set()
        # Unlock message handler thread
        self.message_queue.put((EXIT_MESSAGE_HANDLER, self.node_id))

        self.socket.stop()
        for _, n in self.nodes.items():
            n.stop()

        # Stop main threads
        self.listener_thread.join()
        self.runner_thread.join()
        self.message_handler.join()


class BullyNode:
    def __init__(
        self,
        socket: TCPSocket,
        node_name: str,
        node_id: int,
        message_queue: SimpleQueue,
        get_leader_id: Callable[[], int],
    ):
        self.is_running_lock = Lock()
        self.is_running = True
        self.socket = socket
        self.node_name = node_name
        self.node_id = node_id
        self.message_queue = message_queue
        self.get_leader_id = get_leader_id
        self.recv_thread = Thread(target=self._manage_recv)
        self.recv_thread.start()

    def _manage_recv(self):
        try:
            while self._is_running():
                try:
                    message = self.socket.recv(MESSAGE_BYTES_AMOUNT)

                    self.message_queue.put((message, self.node_id))
                except TimeoutError:
                    if self.get_leader_id == self.node_id:
                        self.message_queue.put(
                            (BullyProtocol.TIMEOUT_REPLY, self.node_id)
                        )
        except Exception as e:
            logger.error(f'Error ocurred while recv message from {self.node_name}: {e}')

    def send(self, message: BullyProtocol):
        self.socket.send(message, MESSAGE_BYTES_AMOUNT)

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        if self.socket:
            self.socket.stop()

        self.recv_thread.join()
