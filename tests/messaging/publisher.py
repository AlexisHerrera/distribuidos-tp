from src.messaging.connection_creator import ConnectionCreator
from src.utils.config import Config


def main():
    config = Config()
    config.rabbit_host = 'rabbitmq'
    config.publisher_exchange = 'test'
    config.consumer_exchange = 'another-test'

    conn = ConnectionCreator.create(config)

    conn.send(b'holi')

    conn.close()


if __name__ == "__main__":
    main()
