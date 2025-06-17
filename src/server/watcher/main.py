import logging

from src.server.watcher.watcher import Watcher
from src.utils.config import WatcherConfig
from src.utils.log import initialize_log

logger = logging.get_logger(__name__)


def main():
    config = WatcherConfig()
    initialize_log(config.log_level)
    watcher = Watcher(
        config.server_port, config.client_port, config.nodes, config.timeout
    )

    try:
        watcher.run()
    except Exception as e:
        logger.error(f'Error ocurred while running watcher: {e}')
    finally:
        watcher.stop()


if __name__ == '__main__':
    main()
