import logging

from src.model.cast import Cast
from src.model.movie import Movie
from src.model.movie_cast import MovieCast
from src.server.joiners.base_joiner_logic import BaseJoinerLogic
from src.messaging.protocol.message import Message, MessageType

logger = logging.getLogger(__name__)


class CastJoinerLogic(BaseJoinerLogic):
    def __init__(self):
        self.base_data: dict[int, Movie] = {}
        logger.info('CastJoinerLogic initialized.')

    def merge(self, message: Message) -> Message:
        cast_list: list[Cast] = message.data
        joined = []
        for cast in cast_list:
            movie = self.base_data.get(cast.id)
            if movie is None:
                # logger.warning(f'No movie in base_data for id={rating.movie_id}')
                continue
            joined.append(
                MovieCast(movie_id=movie.id, title=movie.title, actors_name=cast.cast)
            )
        if len(joined) > 0:
            logger.info(f'Joining {len(joined)} MovieCast')
        return Message(MessageType.MovieCast, joined)
