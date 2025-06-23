import logging
from enum import Enum
from threading import Lock
from typing import Callable

logger = logging.getLogger(__name__)


class BullyState(Enum):
    RUNNING = 1
    WAITING_COORDINATOR = 2
    ELECTION = 3
    END = 4


class BullyStateManager:
    def __init__(self, initial_state: BullyState, node_id: int):
        self.lock = Lock()
        self.state = initial_state
        self.node_id = node_id

    def set_state(self, state: BullyState):
        with self.lock:
            self.state = state

    def is_in_state(self, state):
        with self.lock:
            return self.state == state

    def election(self, init_election: Callable[[], None]):
        with self.lock:
            if self.state != BullyState.ELECTION:
                old_state = self.state
                self.state = BullyState.ELECTION
                init_election()
                logger.debug(
                    f'[BULLY] Election from {old_state.name} to {self.state.name}'
                )

    def answer(self):
        with self.lock:
            if self.state == BullyState.ELECTION:
                old_state = self.state
                self.state = BullyState.WAITING_COORDINATOR
                logger.debug(
                    f'[BULLY] Answer from {old_state.name} to {self.state.name}'
                )

    def coordinator(
        self,
        new_leader_node_id: int,
        set_leader: Callable[[int], None],
        send_coordinator: Callable[[], None],
    ):
        with self.lock:
            if self.node_id < new_leader_node_id:
                old_state = self.state
                self.state = BullyState.RUNNING
                set_leader(new_leader_node_id)
                logger.debug(
                    f'[BULLY] Coordinator from {old_state.name} to {self.state.name}'
                )
            else:
                send_coordinator()

    def timeout_reply(
        self,
        is_leader: bool,
        init_election: Callable[[], None],
        set_leader: Callable[[int], None],
        send_coordinator: Callable[[], None],
    ):
        with self.lock:
            old_state = self.state
            match self.state:
                case BullyState.RUNNING:
                    if is_leader:
                        self.state = BullyState.ELECTION
                        init_election()
                case BullyState.ELECTION:
                    self.state = BullyState.RUNNING
                    set_leader(self.node_id)
                    send_coordinator()
            logger.debug(
                f'[BULLY] TimeoutReply from {old_state.name} to {self.state.name}'
            )
