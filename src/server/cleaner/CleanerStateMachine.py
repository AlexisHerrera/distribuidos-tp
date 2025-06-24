from src.common.protocol.batch import BatchType
from src.messaging.protocol.message import MessageType
from src.utils.StateMachine import StateMachine


class CleanerStateMachine(StateMachine):
    STAGES = ['awaiting_movies', 'awaiting_credits', 'awaiting_ratings', 'finished']
    FINISHED_STAGE = 'finished'

    STAGE_CONFIG = {
        'awaiting_movies': {
            'batch_type': BatchType.MOVIES,
            'message_type': MessageType.Movie,
        },
        'awaiting_credits': {
            'batch_type': BatchType.CREDITS,
            'message_type': MessageType.Cast,
        },
        'awaiting_ratings': {
            'batch_type': BatchType.RATINGS,
            'message_type': MessageType.Rating,
        },
    }

    BATCH_TO_STAGE = {
        BatchType.MOVIES: 'awaiting_movies',
        BatchType.CREDITS: 'awaiting_credits',
        BatchType.RATINGS: 'awaiting_ratings',
    }

    def __init__(self, current_stage: str = 'awaiting_movies'):
        super().__init__(
            stages=self.STAGES,
            stage_config=self.STAGE_CONFIG,
            initial_stage=current_stage,
            finished_stage=self.FINISHED_STAGE,
        )
