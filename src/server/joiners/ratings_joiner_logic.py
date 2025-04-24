import logging

from src.server.joiners.base_joiner_logic import BaseJoinerLogic
from src.messaging.protocol.message import Message

logger = logging.getLogger(__name__)


class RatingsJoinerLogic(BaseJoinerLogic):
    def __init__(self):
        self.base_data: dict
        logger.info('RatingsJoinerLogic initialized.')

    def process_message(self, message: Message):
        pass
        # Message is from ratings queue
        # type is MessageType.Rating
        # Object Rating is like
        # class Rating:
        #     def __init__(self, movie_id: int, rating: float):
        #         self.movie_id = movie_id
        #         self.rating = rating
        # Should merge from Movie by id.
        # and send object
        # class MovieRating:
        #     def __init__(self, movie_id: int, title: str, rating: float):
        #         self.movie_id = movie_id
        #         self.title = title
        #         self.rating = rating
