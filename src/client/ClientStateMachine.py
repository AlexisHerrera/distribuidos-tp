from enum import Enum
from src.common.protocol.batch import BatchType
from src.utils.StateMachine import StateMachine


class ClientState(Enum):
    SENDING_MOVIES = 1
    SENDING_CREDITS = 2
    SENDING_RATINGS = 3
    WAITING_RESULTS = 4
    DONE = 5


class ClientStateMachine(StateMachine):
    STAGES = [
        ClientState.SENDING_MOVIES,
        ClientState.SENDING_CREDITS,
        ClientState.SENDING_RATINGS,
        ClientState.WAITING_RESULTS,
        ClientState.DONE,
    ]
    FINISHED_STAGE = ClientState.DONE

    def __init__(
        self,
        initial_stage: ClientState = ClientState.SENDING_MOVIES,
        args=None,
        BATCH_SIZES=None,
    ):
        stage_config = {
            ClientState.SENDING_MOVIES: {
                'file': args.movies_path,
                'batch_type': BatchType.MOVIES,
                'batch_size': BATCH_SIZES['MOVIES'],
            },
            ClientState.SENDING_CREDITS: {
                'file': args.cast_path,
                'batch_type': BatchType.CREDITS,
                'batch_size': BATCH_SIZES['CREDITS'],
            },
            ClientState.SENDING_RATINGS: {
                'file': args.ratings_path,
                'batch_type': BatchType.RATINGS,
                'batch_size': BATCH_SIZES['RATINGS'],
            },
        }

        super().__init__(
            stages=self.STAGES,
            stage_config=stage_config,
            initial_stage=initial_stage,
            finished_stage=self.FINISHED_STAGE,
        )
