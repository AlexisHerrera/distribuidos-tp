import logging
from queue import SimpleQueue
from threading import Event, Lock, Thread
from typing import Callable

from src.common.runnable import Runnable
from src.messaging.protocol.bully import BullyProtocol
from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket
from src.server.watcher.bully.node import BullyNode
from src.utils.util import IncrementerStop

logger = logging.getLogger(__name__)

INITIAL_VALUE = 0
MAX_TRIES = 3


class NodeConnectionManager:
    def __init__(
        self,
        port: int,
        message_queue: SimpleQueue,
        node_id: int,
        peers: list[str],
        timeout: int,
        all_peers_connected: Event,
        is_leader: Callable[[int], bool],
    ):
        self.is_running = Runnable()
        self.port = port
        self.server: ServerSocket = ServerSocket(self.port, backlog=len(peers) + 1)

        self.peers = peers

        self.message_queue: SimpleQueue = message_queue
        self.node_id = node_id

        self.nodes_lock: Lock = Lock()
        self.nodes: dict[str, BullyNode] = {}

        self.timeout = timeout
        self.is_leader = is_leader
        self.all_peers_connected = all_peers_connected

        self.listener = Thread(target=self._listener)
        self.listener.start()

    def _get_node_id_by_name(self, node_name: str) -> int:
        return self.peers[node_name]

    def _get_node_name_by_id(self, node_id: int) -> str:
        for node_name, v in self.peers.items():
            if v == node_id:
                return node_name

    def _listener(self):
        self._connect_to_peers()

        try:
            while self.is_running():
                try:
                    client_socket, addr = self.server.accept()

                    node_name = TCPSocket.gethostbyaddress(addr)
                    node_id = self._get_node_id_by_name(node_name)

                    logger.debug(
                        f'[BULLY] Node {node_id} ({node_name}) connected to node {self.node_id}'
                    )

                    def _raise():
                        raise Exception

                    self._add_node(
                        client_socket, node_name, node_id, reconnect=lambda _: _raise()
                    )
                except ConnectionAbortedError as e:
                    logger.error(f'[BULLY] Error while accepting client: {e}')
                except TimeoutError:
                    continue
        except Exception as e:
            logger.error(
                f'[BULLY] Error while listening for connections in node_id {self.node_id}: {e}'
            )

    def _add_node(
        self,
        socket: TCPSocket,
        node_name: str,
        node_id: int,
        reconnect: Callable[[], None],
    ):
        with self.nodes_lock:
            if node_name in self.nodes:
                self.nodes[node_name].stop()

            node = BullyNode(
                socket,
                node_name,
                node_id,
                self.message_queue,
                self.node_id,
                reconnect,
                self.is_leader,
            )

            self.nodes[node_name] = node
            logger.debug(f'[BULLY] Nodes {self.nodes.keys()}')
            n_nodes = len(self.nodes)

        if n_nodes == len(self.peers):
            self.all_peers_connected.set()

        logger.info(f'[BULLY] Added node {node_name} {node_id}')

    def _connect_to_peers(self):
        if len(self.peers) == 0:
            return

        sorted_peers = sorted(
            [(name, nid) for name, nid in self.peers.items()],
            key=lambda node: node[1],
        )

        for node_name, node_id in sorted_peers:
            # Only connect to nodes with higher ID
            if self.node_id > node_id:
                continue

            condition = IncrementerStop(INITIAL_VALUE, MAX_TRIES)

            socket = TCPSocket.connect_while_condition(
                node_name,
                self.port,
                condition=condition.run,
                timeout=self.timeout,
            )

            logger.debug(f'[BULLY] Connected to {node_name}')

            self._add_node(socket, node_name, node_id, self.reconnect)

    def reconnect(self, node_name: int) -> TCPSocket:
        return TCPSocket.connect_while_condition(
            node_name, self.port, condition=self.is_running, timeout=self.timeout
        )

    def send(self, node_name: str, message: BullyProtocol):
        with self.nodes_lock:
            try:
                self.nodes[node_name].send(message)
            except Exception as e:
                logger.error(f'[BULLY] Error send message to {node_name}: {e}')

    def send_higher(self, message: BullyProtocol):
        with self.nodes_lock:
            for _, node in self.nodes.items():
                try:
                    if node.node_id > self.node_id:
                        node.send(BullyProtocol.ELECTION)
                except Exception as e:
                    logger.error(
                        f'[BULLY] Error send_higher message to {node.node_name}: {e}'
                    )

    def send_all(self, message: BullyProtocol):
        with self.nodes_lock:
            for _, node in self.nodes.items():
                try:
                    node.send(BullyProtocol.COORDINATOR)
                except Exception as e:
                    logger.error(
                        f'[BULLY] Error send_all message to {node.node_name}: {e}'
                    )

    def send_by_id(self, node_id: int, message: BullyProtocol):
        node_name = self._get_node_name_by_id(node_id)
        self.send(node_name, message)

    def stop(self):
        logger.info(f'[BULLY] Stopping connection manager {self.node_id}')
        self.is_running.stop()

        self.server.stop()

        with self.nodes_lock:
            for _, node in self.nodes.items():
                node.stop()

        self.listener.join()
        logger.info(f'[BULLY] Stopped connection manager {self.node_id}')
