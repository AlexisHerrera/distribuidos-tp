import logging
from concurrent.futures import ThreadPoolExecutor

from src.messaging.protocol.message import Message, MessageType
from src.model.movie import Movie
from src.server.base_node import BaseNode
from src.server.sentiment_analyzer.sentiment_analyzer import SentimentAnalyzer
from src.utils.config import Config

logger = logging.getLogger(__name__)
AVAILABLE_ANALYZER_LOGICS = {'sentiment': SentimentAnalyzer}


class SentimentAnalyzerNode(BaseNode):
    def __init__(self, config: Config, analyzer_type: str):
        super().__init__(config, analyzer_type)
        self._executor = ThreadPoolExecutor(max_workers=4)
        logger.info(f"SentimentAnalyzerNode '{analyzer_type}' initialized.")

    def _get_logic_registry(self):
        return AVAILABLE_ANALYZER_LOGICS

    def _start_eof_monitor(self):
        if not self.leader.enabled:
            return
        self.leader.wait_for_eof()

        logger.info('EOF detected by monitor — waiting for in-flight tasks…')
        self._executor.shutdown(wait=True)
        logger.info('All in-flight tasks completed.')
        super()._start_eof_monitor()

    def handle_message(self, message):
        if not self.is_running():
            return

        if message.message_type == MessageType.Movie:
            self._executor.submit(self._process_and_publish, message.data)
        else:
            logger.warning(f'Unknown message type received: {message.message_type}')

    def _process_and_publish(self, movies: list[Movie]):
        result = self.logic.handle_message(movies)
        out = Message(MessageType.MovieSentiment, result)
        self.connection.thread_safe_send(out)


if __name__ == '__main__':
    SentimentAnalyzerNode.main(AVAILABLE_ANALYZER_LOGICS)
