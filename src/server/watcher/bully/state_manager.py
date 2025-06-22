from enum import Enum
from threading import Lock
from typing import Callable


class BullyState(Enum):
    RUNNING = 1
    WAITING_COORDINATOR = 2
    ELECTION = 3


class BullyStateManager:
    def __init__(self, initial_state: BullyState, node_id: int):
        self.lock = Lock()
        self.state = initial_state
        self.node_id = node_id

    def set_state(self, state: BullyState):
        with self.lock:
            self.state = state

    def get_state(self) -> BullyState:
        with self.lock:
            return self.state

    def is_in_state(self, state):
        with self.lock:
            return self.state == state

    def election(self, init_election: Callable[[], None]):
        with self.lock:
            if self.state != BullyState.ELECTION:
                self.state = BullyState.ELECTION
                init_election()

    def answer(self):
        with self.lock:
            if self.state == BullyState.ELECTION:
                self.state = BullyState.WAITING_COORDINATOR

    def coordinator(self, new_leader_node_id: int, set_leader: Callable[[int], None]):
        with self.lock:
            if (
                self.state == BullyState.WAITING_COORDINATOR
                and self.node_id < new_leader_node_id
            ):
                self.state = BullyState.RUNNING
                set_leader(new_leader_node_id)

    def timeout_reply(
        self,
        init_election: Callable[[], None],
        set_leader: Callable[[int], None],
        send_coordinator: Callable[[], None],
    ):
        with self.lock:
            match self.state:
                case BullyState.RUNNING:
                    self.state = BullyState.ELECTION
                    init_election()
                case BullyState.ELECTION:
                    self.state = BullyState.RUNNING
                    set_leader(self.node_id)
                    send_coordinator()
