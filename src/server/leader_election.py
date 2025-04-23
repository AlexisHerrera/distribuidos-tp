import threading
import socket
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)


class LeaderElection:
    def __init__(self, config: Config):
        self.enabled = config.replicas_enabled
        if not self.enabled:
            logger.info('LeaderElection disabled.')
            return
        self.node_id = config.node_id
        self.port = int(config.port)
        self.peers = [tuple(p.split(':')) for p in config.peers]
        logger.info(f'Peers: {self.peers}')
        self.is_leader = False
        self.eof_event = threading.Event()
        self.done_event = threading.Event()
        self._done_count = 0
        self._total_followers = len(self.peers)
        self.leader_addr = None

        self._sock = socket.socket()
        self._sock.bind(('', self.port))
        self._sock.listen()
        self._stop = threading.Event()
        threading.Thread(target=self._listen, daemon=True).start()
        logger.info(f'LeaderElection listening on port {self.port}')

    def _listen(self):
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
                data = conn.recv(1024)
                conn.close()
                if data.startswith(b'EOF|'):
                    logger.info(f'Decoded:${data.decode()}')
                    _, host, port = data.decode().split('|')
                    self.leader_addr = (host, int(port))
                    self._on_peer_eof()
                elif data == b'DONE':
                    self._on_done()
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f'LeaderElection listen error: {e}')
                break
        self._sock.close()

    def _on_done(self):
        self._done_count += 1
        logger.info(f'Received DONE ({self._done_count}/{self._total_followers})')
        if self._done_count >= self._total_followers:
            self.done_event.set()

    def _on_peer_eof(self):
        if not self.eof_event.is_set():
            logger.info('Peer notification: EOF received, notifying monitor')
        self.eof_event.set()

    def on_local_eof(self):
        if not self.eof_event.is_set():
            self.is_leader = True
            logger.info('I am the LEADER')
        self.eof_event.set()
        payload = f'EOF|{self.node_id}|{self.port}'.encode()
        self.notify_peers(payload)

    def wait_for_eof(self, timeout=None):
        return self.eof_event.wait(timeout)

    def wait_for_done(self, timeout=None):
        return self.done_event.wait(timeout)

    def notify_peers(self, msg: bytes):
        for host, port in self.peers:
            try:
                with socket.create_connection((host, int(port)), timeout=5) as s:
                    s.sendall(msg)
            except Exception as e:
                logger.error(f'Failed notifying {host}:{port}: {e}')

    def send_done(self):
        if not self.leader_addr:
            logger.error('No leader address known; cannot send DONE')
            return
        leader_host, leader_port = self.leader_addr
        try:
            with socket.create_connection((leader_host, leader_port), timeout=5) as s:
                s.sendall(b'DONE')
        except Exception as e:
            logger.error(f'Error notifying DONE to leader: {e}')

    def stop(self):
        self._stop.set()
