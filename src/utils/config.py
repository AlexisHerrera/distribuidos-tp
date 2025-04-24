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

        self.rabbit_host = os.getenv('RABBIT_HOST', rabbit_config['host'])

        self.consumers = connection['consumer']
        self.publishers = connection['publisher']
        self.base_db = connection.get('base_db', {})

        self.log_level = os.getenv('LOG_LEVEL', log_config['level'])

        # Replication config
        self.node_id = os.getenv('NODE_ID', '')
        self.port = os.getenv('PORT', '')
        self.peers = os.getenv('PEERS', '').split(',')
        self.replicas_enabled = os.getenv('PEERS', '') != ''

    def get_env_var(self, var_name: str, default: str = None) -> str | None:
        return os.getenv(var_name, default)


def print_config(config: Config):
    logger.debug(
        f'Config - Rabbit Host: {config.rabbit_host}, '
        f'Log Level: {config.log_level}, '
        f'Replication Enabled: {config.peers != ""}, '
        f'Node ID: {config.node_id}, '
        f'Peers: {config.peers}, '
        f'Listen Port: {config.port}'
    )
