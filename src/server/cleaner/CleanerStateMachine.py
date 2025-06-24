from src.messaging.protocol.message import MessageType
from src.utils.StateMachine import StateMachine


class CleanerStateMachine(StateMachine):
    # Definimos las constantes a nivel de clase para mayor claridad.
    STAGES = ['awaiting_movies', 'awaiting_credits', 'awaiting_ratings', 'finished']
    FINISHED_STAGE = 'finished'

    STAGE_CONFIG = {
        'awaiting_movies': {'label': 'MOVIES', 'message_type': MessageType.Movie},
        'awaiting_credits': {'label': 'CREDITS', 'message_type': MessageType.Cast},
        'awaiting_ratings': {'label': 'RATINGS', 'message_type': MessageType.Rating},
    }

    def __init__(self, current_stage: str = 'awaiting_movies'):
        super().__init__(
            stages=self.STAGES,
            stage_config=self.STAGE_CONFIG,
            initial_stage=current_stage,
            finished_stage=self.FINISHED_STAGE,
        )
