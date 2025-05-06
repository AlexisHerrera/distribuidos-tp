import logging
import socket
import threading
from src.utils.config import Config

logger = logging.getLogger(__name__)


class LeaderElection:
    def __init__(self, config: Config, node_instance):
        self.node = node_instance
        self.node_id = config.node_id
        self.port = int(config.port)
        self.peers = [tuple(p.split(':')) for p in config.peers]

        logger.info(f'Peers: {self.peers}')
        self._total_followers = len(self.peers)
        self.client_states = {}
        self.client_states_lock = threading.Lock()

        self._sock = socket.socket()
        self._sock.bind(('', self.port))
        self._sock.listen()
        self._stop = threading.Event()
        self._listen_thread = threading.Thread(target=self._listen, daemon=True)
        self._listen_thread.start()

        logger.info(f'LeaderElection listening on port {self.port}')

    def _listen(self):
        while not self._stop.is_set():
            try:
                conn, addr = self._sock.accept()
                data = conn.recv(1024)
                decoded_data = data.decode()
                logger.debug(f'Received peer message: {decoded_data}')

                if decoded_data.startswith('EOF|'):
                    _, leader_host, leader_port, user_id_str = decoded_data.split('|')
                    self._on_peer_eof(leader_host, int(leader_port), int(user_id_str))
                elif decoded_data.startswith('DONE|'):
                    _, user_id_str = decoded_data.split('|')
                    self._on_done(int(user_id_str))
                else:
                    logger.warning(f'Received unknown peer message: {decoded_data}')

            except socket.timeout:
                continue
            except Exception as e:
                if not self._stop.is_set():
                    logger.error(f'LeaderElection listen error: {e}', exc_info=True)
                break
            finally:
                conn.close()
        self._sock.close()

    def _get_or_create_client_state(self, user_id: int):
        with self.client_states_lock:
            if user_id not in self.client_states:
                self.client_states[user_id] = {
                    'status': 'running',  # 'running', 'eof_received', 'finalizing', 'done'
                    'is_leader': None,  # True, False, None
                    'leader_addr': None,
                    'done_event': threading.Event(),
                    'done_count': 0,
                    'peers_notified_eof': False,
                }
            return self.client_states[user_id]

    def handle_incoming_eof(self, user_id: int):
        state = self._get_or_create_client_state(user_id)

        with self.client_states_lock:
            state['status'] = 'eof_received'
            if state['is_leader'] is None:
                state['is_leader'] = True
                state['leader_addr'] = (self.node_id, self.port)
                logger.info(f'Node {self.node_id} is the LEADER for user_id {user_id}')
            else:
                logger.warning(
                    f'Node {self.node_id} already received an EOF for {user_id}'
                )

            if not state['peers_notified_eof']:
                payload = f'EOF|{self.node_id}|{self.port}|{user_id}'.encode()
                self.notify_peers(payload)
                state['peers_notified_eof'] = True
                self._trigger_finalization_logic(user_id)
            else:
                logger.warning(
                    f'Already notified peers of EOF for {user_id}, not doing anything else'
                )

    def _trigger_finalization_logic(self, user_id: int):
        threading.Thread(
            target=self._finalize_client, args=(user_id,), daemon=True
        ).start()

    def _finalize_client(self, user_id: int):
        state = self._get_or_create_client_state(user_id)
        logger.info(f'User {user_id}: Starting finalization process.')
        logger.info(f'User {user_id}: Waiting for executor tasks to finish...')
        self.node.wait_for_executor()
        logger.info(f'User {user_id}: Executor tasks finished.')

        self.node.send_final_results(user_id)

        if state['is_leader']:
            wait_successful = self.wait_for_done(user_id, timeout=60)
            if wait_successful:
                logger.info(
                    f'User {user_id}: All followers DONE (or none required); propagating EOF downstream'
                )
                self.node.propagate_eof(user_id)
            else:
                logger.error(
                    f'User {user_id}: Timed out waiting for DONE messages from followers. EOF will not be propagated.'
                )
        else:
            logger.info(f'User {user_id}: Follower sending DONE to leader')
            self.send_done(user_id)

        with self.client_states_lock:
            state['status'] = 'done'

    def _on_done(self, user_id: int):
        state = self._get_or_create_client_state(user_id)
        with self.client_states_lock:
            if not state['is_leader']:
                logger.warning(
                    f"Received DONE for user_id {user_id}, but I'm not the leader. Ignoring."
                )
                return
            state['done_count'] += 1
            logger.info(
                f'User {user_id}: Received DONE ({state["done_count"]}/{self._total_followers})'
            )
            if state['done_count'] >= self._total_followers:
                logger.info(f'User {user_id}: All followers reported DONE.')
                state['done_event'].set()

    def _on_peer_eof(self, leader_host, leader_port, user_id: int):
        state = self._get_or_create_client_state(user_id)
        leader_addr = (leader_host, int(leader_port))
        with self.client_states_lock:
            state['status'] = 'eof_received'
            state['is_leader'] = False
            state['leader_addr'] = leader_addr
            logger.info(
                f'Node {self.node_id} is FOLLOWER for user_id {user_id} (peer EOF first)'
            )
            self._trigger_finalization_logic(user_id)

    def wait_for_done(self, user_id: int, timeout=None):
        state = self._get_or_create_client_state(user_id)
        logger.info(
            f'User {user_id}: Leader waiting for DONE from {self._total_followers} followers...'
        )
        return state['done_event'].wait(timeout)

    def notify_peers(self, msg: bytes):
        for host, port in self.peers:
            try:
                with socket.create_connection((host, int(port)), timeout=5) as s:
                    s.sendall(msg)
            except Exception as e:
                logger.error(f'Failed notifying {host}:{port}: {e}')

    def send_done(self, user_id: int):
        state = self._get_or_create_client_state(user_id)
        leader_addr = state.get('leader_addr')
        if not leader_addr:
            logger.error(f'User {user_id}: No leader address known; cannot send DONE')
            return

        logger.info(f'User {user_id}: Follower sending DONE to leader at {leader_addr}')
        payload = f'DONE|{user_id}'.encode()
        try:
            with socket.create_connection(leader_addr, timeout=5) as s:
                s.sendall(payload)
        except Exception as e:
            logger.error(
                f'User {user_id}: Error sending DONE to leader {leader_addr}: {e}'
            )

    def stop(self):
        if self._stop:
            self._stop.set()

        if self._listen_thread:
            self._listen_thread.join()
