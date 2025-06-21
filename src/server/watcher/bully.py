import logging
from queue import SimpleQueue
from threading import Event, Lock, Thread
from typing import Callable

from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket

logger = logging.getLogger(__name__)


class BullyProtocol:
    ALIVE = b'A'
    ELECTION = b'E'
    ANSWER = b'W'
    COORDINATOR = b'C'


MESSAGE_BYTES_AMOUNT = 1


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
            self.bully_port, backlog=len(peers) + 1
        )

        self.peers = peers

        self.leader_lock = Lock()
        self.leader = max(max(peers.values()), node_id)
        self.change_leader: Event = Event()
        self.node_id = node_id
        logger.info(f'Leader is node_id: {self.leader}')
        self.as_leader = as_leader
        self.as_follower = as_follower

        self.nodes_lock: Lock = Lock()
        self.nodes: dict[str, BullyNode] = {}

        self.is_running_lock = Lock()
        self.is_running = True

        self.message_queue: SimpleQueue = SimpleQueue()
        self.listener_thread = Thread(target=self._listener)
        self.listener_thread.start()

    def am_i_leader(self) -> bool:
        with self.leader_lock:
            return self.leader == self.node_id

    def _set_leader(self, node_id: int):
        logger.info(f'Setting new leader {node_id}')
        with self.leader_lock:
            self.leader = node_id

        self.change_leader.set()

    def _add_node(self, socket: TCPSocket, node_name: str, node_id: int):
        with self.nodes_lock:
            if node_name in self.nodes:
                self.nodes[node_name].stop()

            node = BullyNode(socket, node_name, node_id, self.message_queue)

            self.nodes[node_name] = node

    def _get_node_id_by_name(self, node_name: str) -> int:
        return self.peers[node_name]

    def _listener(self):
        while self._is_running():
            client_socket, addr = self.socket.accept()

            node_name = TCPSocket.gethostbyaddress(addr)
            node_id = self._get_node_id_by_name(node_name)

            self._add_node(client_socket, node_name, node_id)

    def _connect_to_peers(self):
        for node_name, node_id in self.peers.items():
            socket = TCPSocket.connect_while_condition(
                node_name, self.bully_port, condition=self._is_running
            )

            self._add_node(socket, node_name, node_id)

    def run(self):
        self._connect_to_peers()

        while self._is_running():
            (message, node_id) = self.message_queue.get()

            match message:
                case BullyProtocol.ALIVE:
                    # TODO: maybe use heartbeater for this?
                    pass
                case BullyProtocol.ELECTION:
                    # Begin election
                    # if self.state != ELECTION:
                    # self.state = ELECTION
                    # for n in self.nodes:
                    #     if n.node_id > self.node_id:
                    #         n.send(ELECTION)
                    #     else:
                    #         n.send(ANSWER)
                    pass
                case BullyProtocol.ANSWER:
                    # Wait for coordinator message
                    # self.state = WAITING_COORDINATOR
                    pass
                case BullyProtocol.COORDINATOR:
                    # Set new leader
                    # self.state = RUNNING
                    # self.leader = node_id
                    pass

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        self.socket.stop()
        for _, (client, socket) in self.clients_threads.items():
            socket.stop()
            client.join()

        # Stop main threads
        self.listener_thread.join()
        self.runner_thread.join()


class BullyNode:
    def __init__(
        self,
        socket: TCPSocket,
        node_name: str,
        node_id: int,
        message_queue: SimpleQueue,
    ):
        self.is_running_lock = Lock()
        self.is_running = True
        self.socket = socket
        self.node_name = node_name
        self.node_id = node_id
        self.message_queue = message_queue
        self.recv_thread = Thread(target=self._manage_recv)
        self.recv_thread.start()

    def _manage_recv(self):
        try:
            while self._is_running():
                message = self.socket.recv(MESSAGE_BYTES_AMOUNT)

                self.message_queue.put((message, self.node_name))
        except Exception as e:
            logger.error(f'Error ocurred while recv message from {self.node_name}: {e}')

    def send(self, message: BullyProtocol):
        self.socket.send(message.value, MESSAGE_BYTES_AMOUNT)

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        if self.socket:
            self.socket.stop()

        self.recv_thread.join()
