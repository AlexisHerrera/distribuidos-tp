import logging
import os
from configparser import ConfigParser

import yaml

logger = logging.getLogger(__name__)


class Config:
    def __init__(self, filename: str = 'config.ini'):
        config = ConfigParser(os.environ)

        if os.path.exists('/app/config.yaml'):
            filename = 'config.yaml'
            logger.info(f'Loading from {filename}')
            self.__load_from_yaml(filename)
        else:
            logger.info(f'Loading from {filename}')
            config.read(filename)

            self.rabbit_host = os.getenv(
                'RABBIT_HOST', config['DEFAULT']['RABBIT_HOST']
            )
            self.publisher_exchange = os.getenv(
                'PUBLISHER_EXCHANGE', config['DEFAULT'].get('PUBLISHER_EXCHANGE')
            )
            self.consumer_exchange = os.getenv(
                'CONSUMER_EXCHANGE', config['DEFAULT'].get('CONSUMER_EXCHANGE')
            )
            self.output_queue = os.getenv(
                'OUTPUT_QUEUE', config['DEFAULT'].get('OUTPUT_QUEUE')
            )
            self.input_queue = os.getenv(
                'INPUT_QUEUE', config['DEFAULT'].get('INPUT_QUEUE')
            )

            self.log_level = os.getenv(
                'LOG_LEVEL', config['DEFAULT'].get('LOG_LEVEL', 'INFO')
            )

    def __load_from_yaml(self, filename):
        config = {}
        with open(filename, 'r') as f:
            config = yaml.safe_load(f)

        rabbit_config = config['rabbit']
        exchange_config = config['exchange']
        queue_config = config['queue']
        log_config = config['log']

        self.rabbit_host = os.getenv('RABBIT_HOST', rabbit_config['host'])
        self.publisher_exchange = os.getenv(
            'PUBLISHER_EXCHANGE', exchange_config.get('publisher')
        )
        self.consumer_exchange = os.getenv(
            'CONSUMER_EXCHANGE', exchange_config['consumer']
        )
        self.output_queue = os.getenv('OUTPUT_QUEUE', queue_config['output'])
        self.input_queue = os.getenv('INPUT_QUEUE', queue_config['input'])
        self.log_level = os.getenv('LOG_LEVEL', log_config['level'])

    def get_env_var(self, var_name: str, default: str = None) -> str | None:
        return os.getenv(var_name, default)

    def __str__(self):
        return f'Rabbit host: {self.rabbit_host} - Publisher exchange: {self.publisher_exchange} - Consumer exchange: {self.consumer_exchange} - Output queue: {self.output_queue} - Input queue: {self.input_queue}'


def print_config(config: Config):
    logger.debug(f'Config: {config}')
