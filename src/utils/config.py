import logging
import os

import yaml

logger = logging.getLogger(__name__)


class Config:
    def __init__(self, filename: str = 'config.yaml'):
        config = {}

        with open(filename, 'r') as f:
            config = yaml.safe_load(f)

        rabbit_config = config['rabbit']
        connection = config['connection']
        log_config = config['log']
        replication_config = config.get('replication', {})

        self.rabbit_host = os.getenv('RABBIT_HOST', rabbit_config['host'])

        self.consumers = connection['consumer']
        self.publishers = connection['publisher']

        self.log_level = os.getenv('LOG_LEVEL', log_config['level'])

        # Replication config
        self.replication_enabled = replication_config.get('enabled', False)
        self.node_id = os.getenv('NODE_ID', '')
        self.port = replication_config.get('port', 6000)
        self.peers = replication_config.get('peers', [])

    def get_env_var(self, var_name: str, default: str = None) -> str | None:
        return os.getenv(var_name, default)


def print_config(config: Config):
    logger.debug(
        f'Config - Rabbit Host: {config.rabbit_host}, '
        f'Log Level: {config.log_level}, '
        f'Replication Enabled: {config.replication_enabled}, '
        f'Node ID: {config.node_id}, '
        f'Peers: {config.peers}, '
        f'Listen Port: {config.port}'
    )
