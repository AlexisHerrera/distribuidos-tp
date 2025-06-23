import logging
from typing import Dict, Type

from src.messaging.protocol.message import Message, MessageType
from src.model.actor_count import ActorCount
from src.model.movie_cast import MovieCast
from src.server.base_node import BaseNode
from src.server.splitters.cast_splitter_logic import CastSplitter
from src.utils.config import Config

logger = logging.getLogger(__name__)
AVAILABLE_SPLITTER_LOGICS = {'cast_splitter': CastSplitter}


class SplitterNode(BaseNode):
    def __init__(self, config: Config, filter_type: str):
        super().__init__(config, filter_type)

        # self.logic: BaseSplitterLogic

        logger.info(f"SplitterNode '{filter_type}' initialized.")

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_SPLITTER_LOGICS

    def handle_message(self, message: Message):
        if not self.is_running():
            return

        if message.message_type == MessageType.MovieCast:
            movie_cast_list: list[MovieCast] = message.data
            if not isinstance(movie_cast_list, list):
                logger.warning(f'Expected list, got {type(movie_cast_list)}')
                return

            names_count = {}
            for movie_cast in movie_cast_list:
                if not movie_cast:
                    continue

                for name in movie_cast.actors_name:
                    names_count[name] = names_count.get(name, 0) + 1
            list_to_send = []
            for name, count in names_count.items():
                list_to_send.append(ActorCount(name, count))
            if len(list_to_send) > 0:
                try:
                    self.connection.send(
                        Message(
                            message.user_id,
                            MessageType.ActorCount,
                            list_to_send,
                            message_id=None,
                        )
                    )
                    logger.info(f'Se enviaron {len(list_to_send)}')
                except Exception as e:
                    logger.error(
                        f'Error Publishing movies: {e}',
                        exc_info=True,
                    )
        else:
            logger.warning(f'Unknown message type received: {message.message_type}')


if __name__ == '__main__':
    SplitterNode.main(AVAILABLE_SPLITTER_LOGICS)
