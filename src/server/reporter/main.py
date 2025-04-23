import logging

from src.utils.config import Config
from src.utils.log import initialize_log
from src.messaging.connection_creator import ConnectionCreator
from src.messaging.protocol.message import Message, MessageType
import sys

logger = logging.getLogger(__name__)


class Reporter:
    def __init__(self, config: Config):
        self.config = config
        initialize_log(config.log_level)
        self.is_running = True
        self.connection = ConnectionCreator.create(config)
        logger.info(f"Reporter service initialized.")

    def process_message(self, message: Message):
        try:
            if message.message_type == MessageType.Movie:
                logging.info("Reporter added sink result to list of sink results. Query 1")
            else:
                logger.debug(
                    f'Reporter received unhandled type of message: {message.message_type}'
                )

        except Exception as e:
            logger.error(f'Error while processing message: {e}', exc_info=True)
    
    def run(self):
        logger.info("Running reporter...")
        if not self.is_running():
            logger.critical('Reporter init failed.')
            return
        try:
            logger.info(f"Reporter running. Consuming. Waiting...")

            self.connection.recv(self.process_message)

            logger.info('Reporter loop finished.')
        except KeyboardInterrupt:
            logger.warning('KeyboardInterrupt (CTRL-C)...')
        except Exception as e:
            logger.critical(f'Fatal error during run: {e}', exc_info=True)
        finally:
            self.shutdown()

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
