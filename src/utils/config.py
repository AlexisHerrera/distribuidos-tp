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

        self.consumers = []

        for c in connection['consumer']:
            consumer = {'type': c['type'], 'queue': c['queue']}
            self.consumers.append(consumer)

        self.publishers = []
        for p in connection['publisher']:
            publisher = {'type': p['type'], 'queue': p['queue']}
            self.publishers.append(publisher)

        self.log_level = os.getenv('LOG_LEVEL', log_config['level'])

    def get_env_var(self, var_name: str, default: str = None) -> str | None:
        return os.getenv(var_name, default)

    def __str__(self):
        return f'Rabbit host: {self.rabbit_host} - Publisher exchange: {self.publisher_exchange} - Consumer exchange: {self.consumer_exchange} - Output queue: {self.output_queue} - Input queue: {self.input_queue}'


def print_config(config: Config):
    logger.debug(f'Config: {config}')
