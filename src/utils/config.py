import os
from configparser import ConfigParser


class Config:
    def __init__(self, filename: str = 'config.ini'):
        config = ConfigParser(os.environ)

        config.read(filename)

        self.rabbit_host = os.getenv('RABBIT_HOST', config['DEFAULT']['RABBIT_HOST'])
        self.publisher_exchange = os.getenv(
            'PUBLISHER_EXCHANGE', config['DEFAULT']['PUBLISHER_EXCHANGE']
        )
        self.consumer_exchange = os.getenv(
            'CONSUMER_EXCHANGE', config['DEFAULT']['CONSUMER_EXCHANGE']
        )
        self.output_queue = os.getenv('OUTPUT_QUEUE', config['DEFAULT']['OUTPUT_QUEUE'])
        self.input_queue = os.getenv('INPUT_QUEUE', config['DEFAULT']['INPUT_QUEUE'])

    def get_env_var(self, var_name: str, default: str = None) -> str | None:
        return os.getenv(var_name, default)
