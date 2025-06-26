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
        healthcheck = config['healthcheck']

        self.rabbit_host = os.getenv('RABBIT_HOST', rabbit_config['host'])

        self.consumers = connection['consumer']
        self.publishers = connection['publisher']
        self.base_db = connection.get('base_db', {})

        self.log_level = os.getenv('LOG_LEVEL', log_config['level'])

        # Replication config
        self.node_id = os.getenv('NODE_ID', '')
        self.port = os.getenv('PORT', '')
        self.peers = (
            os.getenv('PEERS', '').split(',') if os.getenv('PEERS', '') != '' else []
        )
        self.replicas_enabled = os.getenv('PEERS', '') != ''

        # Healthcheck config
        self.healthcheck_port = healthcheck['port']

    def get_env_var(self, var_name: str, default: str = None) -> str | None:
        return os.getenv(var_name, default)


class WatcherConfig:
    def __init__(self, filename: str = 'config.yaml'):
        config = {}

        with open(filename, 'r') as f:
            config = yaml.safe_load(f)

        # Healthchecker
        healthchecker = config.get(
            'healthchecker', {'port': 13434, 'timeout': 30, 'reconnection_timeout': 10}
        )
        self.healthchecker_port: int = healthchecker['port']
        self.healthchecker_timeout: int = healthchecker['timeout']
        self.healthchecker_reconnection_timeout: int = healthchecker[
            'reconnection_timeout'
        ]

        # Healthcheck
        healthcheck = config.get('healthcheck', {'port': 13434})
        self.healthcheck_port = healthcheck['port']

        # Bully
        bully = config.get('bully', {'port': 25117, 'timeout': 10})
        self.bully_port: int = bully['port']
        self.bully_timeout: int = bully['timeout']
        self.node_id: int = int(os.getenv('NODE_ID'))  # There must be a NODE_ID

        peers: str = os.getenv('PEERS', '')
        self.peers: dict[str, int] = {}
        if peers != '':
            for p in peers.split(','):
                node_id, node_name = p.split(':')
                self.peers[node_name] = int(node_id)

        filters = config.get('nodes_filters', ['watcher', 'chaos_monkey'])
        self.nodes: list[str] = NodesList(filters=filters).nodes

        self.log_level: str = config.get('log_level', 'INFO')


class ChaosMonkeyConfig:
    def __init__(self, filename: str = 'config.yaml'):
        config = {}
        with open(filename, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        self.__dict__.update(config)

        if 'log_level' not in self.__dict__:
            self.log_level = 'INFO'

        filters = config.get('nodes_filters', ['chaos_monkey'])
        self.nodes = NodesList(filters=filters).nodes


class NodesList:
    def __init__(self, filename: str = 'running_nodes', filters: list[str] = None):
        self.nodes: list[str] = []
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                node_name = clean_line(line)
                if not filter_node(node_name, filters):
                    self.nodes.append(node_name)


def clean_line(line: str):
    return line.replace('\n', '').strip('\n ')


def filter_node(node_name: str, filters: list[str]) -> bool:
    filtered = False

    if not filters or len(filters) == 0:
        return filtered

    for filter in filters:
        if filter in node_name:
            filtered = True
            break

    return filtered


def print_config(config: Config):
    logger.debug(
        f'Config - Rabbit Host: {config.rabbit_host}, '
        f'Log Level: {config.log_level}, '
        f'Replication Enabled: {config.peers != ""}, '
        f'Node ID: {config.node_id}, '
        f'Peers: {config.peers}, '
        f'Listen Port: {config.port}'
    )
