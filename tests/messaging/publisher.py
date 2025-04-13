from src.messaging.connection_creator import ConnectionCreator
from src.utils.config import Config


def main():
    config = Config()

    conn = ConnectionCreator.create(config)

    # movies = list[Movie]
    # conn.send(movies)

    conn.send(b'holi')

    conn.close()


if __name__ == "__main__":
    main()
