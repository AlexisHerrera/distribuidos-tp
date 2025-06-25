import logging
from typing import Any, List, Dict, TypeVar

T_State = TypeVar('T_State')


class StateMachine:
    def __init__(
        self,
        stages: List[T_State],
        stage_config: Dict[T_State, Any],
        initial_stage: T_State,
        finished_stage: T_State,
    ):
        if initial_stage not in stages:
            raise ValueError(f'Invalid initial stage: {initial_stage}')
        if finished_stage not in stages:
            raise ValueError(f'Invalid finished stage: {finished_stage}')

        self.stages = stages
        self.stage_config = stage_config
        self.stage = initial_stage
        self.finished_stage = finished_stage

    def advance(self):
        if self.is_finished():
            logging.warning('Cannot advance: state machine is already finished.')
            return

        current_index = self.stages.index(self.stage)
        if current_index + 1 < len(self.stages):
            self.stage = self.stages[current_index + 1]
            logging.info(f'State advanced to: {repr(self.stage)}')
        else:
            logging.warning('Cannot advance past the final stage.')

    def set_stage(self, new_stage: T_State):
        if new_stage not in self.stages:
            raise ValueError(f'Invalid stage to set: {new_stage}')
        self.stage = new_stage
        logging.info(f'State explicitly set to: {repr(self.stage)}')

    def is_finished(self) -> bool:
        return self.stage == self.finished_stage

    def get_current_stage(self) -> T_State:
        return self.stage

    def get_current_config(self) -> Dict[str, Any] | None:
        return self.stage_config.get(self.stage)
