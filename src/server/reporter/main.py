import logging

from src.utils.config import Config
from src.utils.log import initialize_log
import sys

logger = logging.getLogger(__name__)


class Reporter:
    def __init__(self, config: Config):
        self.config = config
        initialize_log(config.log_level)
        logger.info(f"Reporter service initialized.")
    
    def run(self):
        logger.info("Running reporter...")

    def shutdown(self):
        logger.info("Shutting down reporter service...")


if __name__ == '__main__':
    reporter_instance = None
    try:
        config = Config()
        reporter_instance = Reporter(config)
        reporter_instance.run()
        logger.info('Reporter run method finished.')
        sys.exit(0)
    except ValueError as e:
        logger.critical(f'Failed to initialize Reporter: {e}')
        sys.exit(1)
    except Exception as e:
        logger.critical(
            f'Unhandled exception in main execution block: {e}', exc_info=True
        )
        if reporter_instance:
            reporter_instance.shutdown()
        sys.exit(1)
