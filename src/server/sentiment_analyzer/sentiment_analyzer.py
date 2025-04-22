import logging
import signal

from transformers import pipeline

from src.messaging.connection_creator import ConnectionCreator
from src.messaging.protocol.message import Message, MessageType
from src.model.movie import Movie
from src.model.movie_sentiment import MovieSentiment
from src.utils.config import Config

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    def __init__(self, config: Config):
        self.__connection = ConnectionCreator.create(config)
        self.__running = True
        self.__analyzer = pipeline(
            'sentiment-analysis',
            model='distilbert-base-uncased-finetuned-sst-2-english',
        )
        signal.signal(signal.SIGTERM, self._handle_termination)
        signal.signal(signal.SIGINT, self._handle_termination)

    def _handle_termination(self, signum, _frame):
        logger.warning(f'SIGTERM signal received ({signum}). Closing connection...')
        self.running = False

        if self.__connection:
            self.__connection.close()

        logger.warning('Connection closed.')

    def process_message(self, message: Message):
        if not self.__running:
            logger.warning('Message received during closing connection, ignoring.')
            return

        logger.info(f'Message received of type {message.message_type}')

        match message.message_type:
            case MessageType.Movie:
                self.__handle_movies(message.data)
            case MessageType.EOF:
                self.__handle_eof()
            case _:
                logger.warning(f'Unknown message: {message.message_type}')

    def __handle_movies(self, movies: list[Movie]):
        analyzed_movies = []

        for movie in movies:
            sentiment = self.__analyzer(movie.overview)[0]['label']

            movie_sentiment = MovieSentiment(
                movie.id, movie.title, movie.budget, movie.revenue, sentiment
            )

            analyzed_movies.append(movie_sentiment)

        message = Message(MessageType.MovieSentiment, analyzed_movies)

        self.__connection.send(message)

    def __handle_eof(self):
        logger.info('Received EOF')
        self.__connection.send(Message(MessageType.EOF, None))
        logger.info('Propagated EOF')

    def run(self):
        logger.info('Start reading messages...')

        self.__connection.recv(self.process_message)
