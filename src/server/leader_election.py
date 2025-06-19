import json
import logging
import socket
import threading
import uuid
from typing import Any, Callable

from src.utils.config import Config
from src.utils.state_manager import StateManager

logger = logging.getLogger(__name__)


class LeaderElection:
    def __init__(self, config: Config, node_instance):
        self.node = node_instance
        self.node_id = config.node_id
        self.port = int(config.port)
        self.peers: list[tuple[str, str]] = [tuple(p.split(':')) for p in config.peers]
        self.peer_ids: list[str] = [host for host, port in self.peers]

        logger.info(f'Peers: {self.peers}')
        self._total_followers = len(self.peers)
        # Recovering State
        self.state_manager = StateManager()
        self.persisted_states: dict[str, dict[str, Any]] = (
            self.state_manager.load_state()
        )
        self.runtime_states: dict[str, dict[str, Any]] = {}
        self.state_lock = threading.Lock()
        logger.info(
            f'Loaded initial state: {json.dumps(self.persisted_states, indent=2)}'
        )
        self._recover_runtime_state()

        # Network
        self._sock = socket.socket()
        self._sock.bind(('', self.port))
        self._sock.listen()
        self._stop = threading.Event()
        self._listen_thread = threading.Thread(target=self._listen, daemon=True)
        self._listen_thread.start()

        logger.info(f'LeaderElection listening on port {self.port}')

    def _recover_runtime_state(self):
        with self.state_lock:
            for user_id_str, state in self.persisted_states.items():
                if (
                    state.get('role') == 'leader'
                    and state.get('status') == 'waiting_for_dones'
                ):
                    logger.warning(
                        f'Recovery: Node was a leader waiting for DONEs for user {user_id_str}.'
                    )
                    # Asociamos la Condition con el lock principal para máxima seguridad.
                    self.runtime_states[user_id_str] = {
                        'condition': threading.Condition(self.state_lock)
                    }

    def _update_and_persist_state(
        self, user_id_str: str, update_func: Callable[[dict], None]
    ):
        with self.state_lock:
            state = self.persisted_states.get(user_id_str, {})
            update_func(state)
            self.persisted_states[user_id_str] = state
            self.state_manager.save_state(self.persisted_states)

    def _listen(self):
        while not self._stop.is_set():
            try:
                conn, addr = self._sock.accept()
                data = conn.recv(1024)
                decoded_data = data.decode()
                logger.debug(f'Received peer message: {decoded_data}')

                if decoded_data.startswith('EOF|'):
                    _, leader_host, leader_port, user_id_str = decoded_data.split('|')
                    self._on_peer_eof(
                        leader_host, int(leader_port), uuid.UUID(user_id_str)
                    )
                elif decoded_data.startswith('DONE|'):
                    # DONE|<user_id>|<follower_node_id>
                    _, user_id_str, follower_id = decoded_data.split('|')
                    self._on_done(uuid.UUID(user_id_str), follower_id)
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

    def handle_incoming_eof(self, user_id: uuid.UUID):
        """EOF from data queue"""
        user_id_str = str(user_id)

        def create_leader_state(state):
            if not state:
                state.update(
                    {
                        'role': 'leader',
                        'status': 'finalizing',
                        'leader_addr': None,
                        'pending_dones_from': list(self.peer_ids),
                    }
                )
                logger.info(f'Node {self.node_id} is LEADER for user_id {user_id}')

        self._update_and_persist_state(user_id_str, create_leader_state)

        payload = f'EOF|{self.node_id}|{self.port}|{user_id_str}'.encode()
        self.notify_peers(payload)
        # Blocking call
        self._finalize_client(user_id)

    def _on_peer_eof(self, leader_host: str, leader_port: int, user_id: uuid.UUID):
        """EOF from peer, should make me a follower"""
        user_id_str = str(user_id)

        def create_follower_state(state):
            if not state:
                state.update(
                    {
                        'role': 'follower',
                        'status': 'finalizing',
                        'leader_addr': (leader_host, leader_port),
                        'pending_dones_from': [],
                    }
                )
                logger.info(
                    f'Node {self.node_id} is FOLLOWER for {user_id} (leader: {leader_host})'
                )

        self._update_and_persist_state(user_id_str, create_follower_state)
        threading.Thread(target=self._finalize_client, args=(user_id,)).start()

    def _on_done(self, user_id: uuid.UUID, follower_id: str):
        """DONE handler from peer to leader"""
        user_id_str = str(user_id)

        def process_done_update(state):
            if not state or state.get('role') != 'leader':
                return

            pending = state.get('pending_dones_from', [])
            if follower_id in pending:
                pending.remove(follower_id)
                logger.info(
                    f'Received valid DONE for {user_id} from {follower_id}. Remaining: {len(pending)}'
                )
                if not pending:
                    state['status'] = 'all_dones_received'
                    cond = self.runtime_states.get(user_id_str, {}).get('condition')
                    if cond:
                        cond.notify()
            else:
                logger.warning(
                    f'Received duplicate/unexpected DONE for {user_id} from {follower_id}.'
                )

        self._update_and_persist_state(user_id_str, process_done_update)

    def _finalize_client(self, user_id: uuid.UUID):
        user_id_str = str(user_id)
        with self.state_lock:
            state = self.persisted_states.get(user_id_str)
        if not state:
            logger.error(f'Cannot finalize client {user_id}: state not found.')
            return

        is_leader = state['role'] == 'leader'
        logger.info(
            f'User {user_id}: Finalizing as {"LEADER" if is_leader else "FOLLOWER"}.'
        )
        self.node.wait_for_last_user_message(user_id, is_leader)
        self.node.send_final_results(user_id)

        if is_leader:
            with self.state_lock:
                if user_id_str not in self.runtime_states:
                    self.runtime_states[user_id_str] = {
                        'condition': threading.Condition(self.state_lock)
                    }
                cond = self.runtime_states[user_id_str]['condition']

            with cond:
                while self.persisted_states[user_id_str]['pending_dones_from']:
                    logger.info(
                        f'Leader for {user_id} waiting for DONEs from {self.persisted_states[user_id_str]["pending_dones_from"]}...'
                    )
                    if not cond.wait(timeout=60.0):
                        logger.error(
                            f'Timed out waiting for DONEs for user {user_id}. EOF will not be propagated.'
                        )
                        return

            logger.info(
                f'All followers for {user_id} reported DONE; propagating EOF downstream.'
            )
            self.node.propagate_eof(user_id)
        else:
            self.send_done(user_id)

        def mark_as_completed(state):
            state['status'] = 'completed'

        self._update_and_persist_state(user_id_str, mark_as_completed)

    def notify_peers(self, msg: bytes):
        for host, port in self.peers:
            try:
                with socket.create_connection((host, int(port)), timeout=5) as s:
                    s.sendall(msg)
            except Exception as e:
                logger.error(f'Failed notifying {host}:{port}: {e}')

    def send_done(self, user_id: uuid.UUID):
        user_id_str = str(user_id)
        with self.state_lock:
            state = self.persisted_states.get(user_id_str)
        if not state:
            logger.error(f'User {user_id}: No state found; cannot send DONE')
            return

        leader_addr = state.get('leader_addr')
        if not leader_addr:
            logger.error(f'User {user_id}: No leader address known; cannot send DONE')
            return

        payload = f'DONE|{user_id_str}|{self.node_id}'.encode()
        try:
            with socket.create_connection(leader_addr, timeout=5.0) as s:
                s.sendall(payload)
        except Exception as e:
            logger.error(f'Error sending DONE to leader {leader_addr}: {e}')

    def stop(self):
        if self._stop:
            self._stop.set()

        if self._listen_thread:
            self._listen_thread.join()
