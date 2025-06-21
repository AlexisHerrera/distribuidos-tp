import threading
from queue import SimpleQueue

from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket


class BullyProtocol:
    ALIVE = b'A'
    ELECTION = b'E'
    ANSWER = b'W'
    COORDINATOR = b'C'


class Bully:
    def __init__(self, port: int, peers: list[str], node_id: int):
        self.socket: ServerSocket = ServerSocket(port, backlog=len(peers) + 1)
        self.bully_port = port
        # Extract node_id of peers. Peers are named like `watcher-1`.
        peer_ids = [peer.split('-')[-1] for peer in peers]

        self.leader_lock = threading.Lock()
        self.leader = max(max(peer_ids), node_id)
        self.peers = peers

        self.is_running_lock = threading.Lock()
        self.is_running = True

        self.clients_threads = {}
        self.message_queue: SimpleQueue = SimpleQueue()
        self.listener_thread = threading.Thread(target=self._listener)
        self.runner_thread = threading.Thread(target=self._run)
        self.listener_thread.start()
        self.runner_thread.start()

    def am_i_leader(self) -> bool:
        with self.leader_lock:
            return self.leader

    # def _manage_client(self, socket: TCPSocket, addr: tuple):
    #     while self._is_running():
    #         message = socket.recv(1)
    #
    #         self._manage_message_recv(message, addr)
    #         # self.message_queue.put(message)
    #         # TODO: Don't pass message to queue, manage it here/sequentially
    #         # and share/lock state

    def _listener(self):
        while self._is_running():
            client_socket, addr = self.socket.accept()

            self._add_node(client_socket, addr)
            # node = BullyNode()

            client = threading.Thread(
                target=self._manage_client, args=(client_socket, addr)
            )

            self.clients_threads[addr] = (client, client_socket)

            client.start()

    def _manage_message_recv(self, message: bytes, addr: tuple):
        match message:
            case BullyProtocol.ALIVE:
                # TODO: maybe use heartbeater for this?
                pass
            case BullyProtocol.ELECTION:
                # Begin election
                pass
            case BullyProtocol.ANSWER:
                # Wait for coordinator message
                pass
            case BullyProtocol.COORDINATOR:
                # Set new leader
                pass

    def _connect_to_peers(self):
        pass
        # for peer, node_id in self.peers.items():
        #     socket = TCPSocket.connect_while_condition(
        #         peer, self.bully_port, condition=self._is_running
        #     )
        #     # node = BullyNode(socket, node_id, self.message_queue)
        #     # t = threading.Thread(target=node.run)
        #     # t.start()
        #     # self.clients_threads[peer] = (node, t)
        #     self._add_node()

    def _run(self):
        self._connect_to_peers()

        while self._is_running():
            if self.am_i_leader():
                # Do leader-thing
                # Send coordinator message
                # Begin heartbeating nodes
                pass
            else:
                # Do follower-thing
                # Send alive to leader and keep heartbeating other watchers
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
    def __init__(self, socket: TCPSocket, node_id: int, message_queue: SimpleQueue):
        self.is_running_lock = threading.Lock()
        self.is_running = True
        self.socket = socket
        self.node_id = node_id
        self.message_queue = message_queue
        self.recv_thread = threading.Thread(target=self._manage_recv)
        self.recv_thread.start()

    def _manage_recv(self):
        while self._is_running():
            message = self.socket.recv(1)

            self.message_queue.put(message, self.node_id)

    def run(self):
        while self._is_running():
            pass

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        with self.is_running_lock:
            self.is_running = False

        if self.socket:
            self.socket.stop()

        self.recv_thread.join()
