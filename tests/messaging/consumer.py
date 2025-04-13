import signal

from src.utils.log import logging
from src.messaging.connection_creator import ConnectionCreator
from src.utils.config import Config

def callback(body):
    logging.info(f"{body}")


def main():
    config = Config()
    config.host = 'rabbitmq'
    config.consumer_exchange = 'test'
    config.publisher_exchange = 'another-test'

    conn = ConnectionCreator.create(config)

    def stop():
        conn.close()

    signal.signal(signal.SIGTERM, stop)

    conn.recv(callback=callback)


if __name__ == "__main__":
    main()
