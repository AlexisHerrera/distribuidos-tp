import logging

from src.messaging.protocol.message import Message, MessageType
from src.server.base_node import BaseNode
from src.server.sentiment_analyzer.sentiment_analyzer import SentimentAnalyzer
from src.utils.config import Config

logger = logging.getLogger(__name__)
AVAILABLE_ANALYZER_LOGICS = {'sentiment': SentimentAnalyzer}


class SentimentAnalyzerNode(BaseNode):
    def __init__(self, config: Config, analyzer_type: str):
        super().__init__(config, analyzer_type)

        logger.info(f"SentimentAnalyzerNode '{analyzer_type}' initialized.")

    def _get_logic_registry(self):
        return AVAILABLE_ANALYZER_LOGICS

    def handle_message(self, message):
        if not self.is_running():
            return

        if message.message_type == MessageType.Movie:
            result = self.logic.handle_message(message.data)
            out_message = Message(MessageType.MovieSentiment, result)
            self.connection.send(out_message)
        else:
            logger.warning(f'Unknown message type received: {message.message_type}')


if __name__ == '__main__':
    SentimentAnalyzerNode.main(AVAILABLE_ANALYZER_LOGICS)
