import signal

from src.messaging.message import Message
from src.utils.log import initialize_log, logging
from src.messaging.connection_creator import ConnectionCreator
from src.utils.config import Config


def callback(body: Message):
    logging.info(f'{body.message_type} {body.data}')


def main():
    config = Config()
    initialize_log()

    conn = ConnectionCreator.create(config)

    def stop():
        conn.close()

    signal.signal(signal.SIGTERM, stop)

    conn.recv(callback=callback)


if __name__ == '__main__':
    main()
