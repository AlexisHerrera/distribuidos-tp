import logging

from src.server.sentiment_analyzer.sentiment_analyzer import SentimentAnalyzer
from src.utils.config import Config, print_config
from src.utils.log import initialize_log

logger = logging.getLogger(__name__)


def main():
    try:
        config = Config()
        initialize_log(config.log_level)
        print_config(config)

        sentiment_analyzer = SentimentAnalyzer(config)
        sentiment_analyzer.run()
    except Exception as e:
        logger.error(f'{e}')
        return -1


if __name__ == '__main__':
    main()
