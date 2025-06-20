import threading

from src.messaging.server_socket import ServerSocket
from src.messaging.tcp_socket import TCPSocket


class BullyProtocol:
    ALIVE = b'A'
    ELECTION = b'E'
    ANSWER = b'W'
    COORDINATOR = b'C'


class Bully:
    def __init__(self, port: int, peers: list[str]):
        self.socket: ServerSocket = ServerSocket(port, len(peers) + 1)

        self.is_running = True
        self.clients_threads = {}
        self.listener_thread = threading.Thread(target=self.run)
        self.listener_thread.start()

    def _manage_client(self, socket: TCPSocket, addr: tuple):
        while self.is_running:
            message = socket.recv(1)

            match message:
                case BullyProtocol.ALIVE:
                    pass
                case BullyProtocol.ELECTION:
                    # Begin election
                    pass
                case BullyProtocol.ANSWER:
                    pass
                case BullyProtocol.COORDINATOR:
                    # Set new leader
                    pass

    def run(self):
        while self.is_running:
            client_socket, addr = self.socket.accept()

            client = threading.Thread(
                target=self._manage_client, args=(client_socket, addr)
            )
            client.start()

            self.clients_threads[addr] = (client, client_socket)

    def stop(self):
        self.is_running = False
        self.socket.stop()
        for _, (client, socket) in self.clients_threads.items():
            socket.stop()
            client.join()
        self.listener_thread.join()
