import logging
from concurrent.futures import ThreadPoolExecutor

from src.messaging.protocol.message import Message, MessageType
from src.server.base_node import BaseNode
from src.server.sentiment_analyzer.sentiment_analyzer import SentimentAnalyzer
from src.utils.config import Config

logger = logging.getLogger(__name__)
AVAILABLE_ANALYZER_LOGICS = {'sentiment': SentimentAnalyzer}


class SentimentAnalyzerNode(BaseNode):
    def __init__(self, config: Config, analyzer_type: str):
        super().__init__(config, analyzer_type)
        self.__executor = ThreadPoolExecutor(max_workers=1)

        logger.info(f"SentimentAnalyzerNode '{analyzer_type}' initialized.")

    def _get_logic_registry(self):
        return AVAILABLE_ANALYZER_LOGICS

    def handle_message(self, message):
        if not self.is_running():
            return

        if message.message_type == MessageType.Movie:
            logic_result = []

            movies = message.data
            logic_result = self.__executor.map(self.logic.handle_message, [movies])

            result = []
            for r in logic_result:
                result.extend(r)

            out_message = Message(MessageType.MovieSentiment, result)

            self.connection.send(out_message)
        else:
            logger.warning(f'Unknown message type received: {message.message_type}')

    def stop(self):
        self.__executor.shutdown(cancel_futures=True)


if __name__ == '__main__':
    SentimentAnalyzerNode.main(AVAILABLE_ANALYZER_LOGICS)
