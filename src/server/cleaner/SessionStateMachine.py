from typing import Any

from src.messaging.protocol.message import MessageType


class SessionStateMachine:
    STAGES = ['awaiting_movies', 'awaiting_credits', 'awaiting_ratings', 'finished']
    STAGE_CONFIG = {
        'awaiting_movies': {'label': 'MOVIES', 'message_type': MessageType.Movie},
        'awaiting_credits': {'label': 'CREDITS', 'message_type': MessageType.Cast},
        'awaiting_ratings': {'label': 'RATINGS', 'message_type': MessageType.Rating},
    }

    def __init__(self, current_stage: str):
        if current_stage not in self.STAGES:
            raise ValueError(f'Invalid stage: {current_stage}')
        self.stage = current_stage

    def advance(self):
        if self.is_finished():
            return
        current_index = self.STAGES.index(self.stage)
        self.stage = self.STAGES[current_index + 1]

    def is_finished(self) -> bool:
        return self.stage == 'finished'

    def get_current_config(self) -> dict[str, Any] | None:
        return self.STAGE_CONFIG.get(self.stage)
