import threading
import socket
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)


class LeaderElection:
    def __init__(self, config: Config):
        self.enabled = config.replicas_enabled
        self.node_id = config.node_id
        self.port = int(config.port)
        if not self.enabled:
            logger.info('LeaderElection disabled in config.')
            return
        peers = config.peers
        # Parse peers list: ["host:port", ...]
        self.peers = []
        for p in peers:
            host, port = p.split(':')
            self.peers.append((host, int(port)))

        self.is_leader = False
        # Event to signal EOF
        self.eof_event = threading.Event()
        # Shutdown signal for listen thread
        self._stop_event = threading.Event()

        # Start background listener
        self._thread = threading.Thread(target=self._listen, daemon=True)
        self._thread.start()
        logger.info(
            f'LeaderElection: listening on port {self.port}, peers: {self.peers}'
        )

    def _listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', self.port))
        sock.listen()
        sock.settimeout(1.0)
        while not self._stop_event.is_set():
            try:
                conn, addr = sock.accept()
                data = conn.recv(1024)
                if data == b'EOF':
                    logger.info(f'Received EOF from {addr}')
                    self.eof_event.set()
                conn.close()
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f'LeaderElection listen error: {e}')
                break
        sock.close()
        logger.info('LeaderElection listener stopped.')

    def on_local_eof(self):
        """Call when this node receives EOF in process_message."""
        if not self.enabled:
            return

        # Leader election: lowest node_id lexicographically
        candidates = [self.node_id] + [h for h, _ in self.peers]
        logger.info(f'Candidates: {candidates}')
        leader = sorted(candidates)[0]
        if self.node_id == leader:
            self.is_leader = True
            logger.info('Elected as LEADER')
            self._notify_peers()
        else:
            logger.info('Running as FOLLOWER')

        # Signal local EOF
        self.eof_event.set()

    def _notify_peers(self):
        for host, port in self.peers:
            try:
                with socket.create_connection((host, port), timeout=5) as sock:
                    sock.sendall(b'EOF')
                    logger.info(f'Notified peer {host}:{port}')
            except Exception as e:
                logger.error(f'Failed notifying {host}:{port}: {e}')

    def wait_for_eof(self, timeout=None):
        """Block until EOF event is set by this node or peers."""
        return self.eof_event.wait(timeout)

    def stop(self):
        """Stop the background listener thread."""
        self._stop_event.set()
        self._thread.join()
