from src.messaging.connection_creator import ConnectionCreator
from src.messaging.message import MessageType, Message
from src.model.movie import Movie
from src.utils.config import Config
from src.utils.log import initialize_log


def main():
    config = Config()

    conn = ConnectionCreator.create(config)
    initialize_log()

    message = Message(MessageType.Movie, [Movie(1, 'Toy Story'), Movie(2, 'Shrek')])

    conn.send(message)

    conn.close()


if __name__ == '__main__':
    main()
