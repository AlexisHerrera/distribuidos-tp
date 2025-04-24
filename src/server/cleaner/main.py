import logging
import os
import signal
import sys
from collections import defaultdict

from src.common.protocol.batch import Batch, BatchType
from src.common.socket_communication import (
    accept_new_connection,
    create_server_socket,
    receive_message,
)
from src.messaging.connection_creator import ConnectionCreator
from src.messaging.protocol.message import Message, MessageType
from src.model.cast import Cast
from src.model.movie import Movie
from src.model.rating import Rating
from src.server.cleaner.clean_credits import parse_line_to_credits
from src.server.cleaner.clean_movies import parse_line_to_movie
from src.server.cleaner.clean_ratings import parse_line_to_rating
from src.utils.config import Config

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Cleaner:
    def __init__(self, config: Config):
        logger.info('Initializing Cleaner...')
        self.config = config
        self.is_running = True
        self.connection = ConnectionCreator.create_multipublisher(config)
        self.server_socket = None
        self.client_socket = None

        try:
            self.port = int(os.getenv('SERVER_PORT', '12345'))
            self.backlog = int(os.getenv('LISTENING_BACKLOG', '3'))

        except (ValueError, KeyError, AttributeError) as e:
            logger.critical(
                f'Error reading configuration during init: {e}', exc_info=True
            )
            self.is_running = False
            raise ValueError(f'Configuration error: {e}') from e

        logger.info('Cleaner initialized.')

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.shutdown()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Signal handlers configured.')

    def _setup_server_socket(self):
        if not self.is_running:
            return False
        try:
            logger.info(f'Setting up server socket on port {self.port}...')
            self.server_socket = create_server_socket(self.port, self.backlog)
            logger.info('Server socket listening.')
            return True
        except Exception as e:
            logger.critical(f'Failed to set up server socket: {e}', exc_info=True)
            self.is_running = False
            return False

    def _accept_client(self):
        if not self.is_running or not self.server_socket:
            return False
        try:
            logger.info('Waiting for client connection...')
            self.client_socket = accept_new_connection(self.server_socket)
            if self.client_socket:
                logger.info('Client connected.')
                return True
            else:
                logger.warning('Failed to accept client connection.')
                self.is_running = False
                return False
        except Exception as e:
            if self.is_running:
                logger.error(f'Error accepting client connection: {e}', exc_info=True)
            self.is_running = False
            return False

    def batch_to_list_objects(self, batch: Batch) -> list:
        if batch.type == BatchType.MOVIES:
            return self._batch_data_to_movies(batch.data)
        elif batch.type == BatchType.CREDITS:
            return self._batch_data_to_credits(batch.data)
        elif batch.type == BatchType.RATINGS:
            return self._batch_data_to_ratings(batch.data)
        else:
            logger.warning(f'No parser implemented for BatchType: {batch.type.name}')
            return []

    def _batch_data_to_movies(self, data_lines: list[str]) -> list[Movie]:
        parsed_movies = []
        for line in data_lines:
            if not self.is_running:
                break
            if not line.strip():
                continue
            movie = parse_line_to_movie(line)
            if movie:
                parsed_movies.append(movie)

        return parsed_movies

    def _batch_data_to_credits(self, data_lines: list[str]) -> list[Cast]:
        parsed_credits = []
        for line in data_lines:
            if not self.is_running:
                break
            if not line.strip():
                continue
            credits = parse_line_to_credits(line)
            if credits:
                parsed_credits.append(credits)

        return parsed_credits

    def _batch_data_to_ratings(self, data_lines: list[str]) -> list[Rating]:
        parsed_ratings = []
        for line in data_lines:
            if not self.is_running:
                break
            if not line.strip():
                continue
            rating = parse_line_to_rating(line)
            if rating:
                parsed_ratings.append(rating)

        return parsed_ratings

    def _process_client_data(self, type_of_data):
        if not self.is_running or not self.client_socket:
            logger.error(
                'Cannot process client data: component missing or not running.'
            )
            return

        logger.info('Starting to process data from client using Batch protocol...')
        processed_counts = defaultdict(int)
        total_batches = 0

        while self.is_running:
            try:
                raw_batch_bytes = receive_message(self.client_socket)
                if raw_batch_bytes is None:
                    logger.warning('Client connection closed or receive failed.')
                    break

                batch = Batch.from_bytes(raw_batch_bytes)
                total_batches += 1
                if batch is None:
                    logger.error(f'Failed to decode Batch #{total_batches}. Stopping.')
                    self.is_running = False
                    break

                logger.debug(
                    f'Received Batch #{total_batches}: Type={batch.type.name}, Lines={len(batch.data)}'
                )

                if batch.type == BatchType.EOF:
                    logger.info(
                        'EOF Batch received from client. Signaling end of all streams.'
                    )
                    self._publish_eof()
                    break

                object_list = self.batch_to_list_objects(batch)

                message_type_enum = None
                if batch.type == BatchType.MOVIES:
                    message_type_enum = MessageType.Movie
                elif batch.type == BatchType.CREDITS:
                    message_type_enum = MessageType.Cast
                elif batch.type == BatchType.RATINGS:
                    message_type_enum = MessageType.Rating

                if not message_type_enum:
                    logger.warning(
                        f'No target queue or message type defined for BatchType: {batch.type.name}. Skipping batch.'
                    )
                    continue

                count_in_batch = 0
                model_object_list = []
                for model_object in object_list:
                    if not self.is_running:
                        break
                    try:
                        model_object_list.append(model_object)
                        count_in_batch += 1
                    except Exception as e:
                        obj_id = getattr(model_object, 'id', 'N/A')
                        logger.error(
                            f'Error publishing {message_type_enum.name} ID {obj_id}: {e}',
                            exc_info=True,
                        )

                output_message = Message(message_type_enum, model_object_list)
                self.connection.send(output_message)

                if count_in_batch > 0:
                    processed_counts[batch.type] += count_in_batch
                    logger.debug(
                        f'Published {count_in_batch} objects of type {batch.type.name} from Batch #{total_batches}'
                    )

                if not self.is_running:
                    break

            except ConnectionAbortedError:
                logger.warning('Client connection aborted.')
                self.is_running = False
            except Exception as e:
                if self.is_running:
                    logger.error(
                        f'Unexpected error in receive loop: {e}', exc_info=True
                    )
                # self.is_running = False

        logger.info('Client data processing finished.')
        logger.info(f'Processing Summary: {dict(processed_counts)}')

    def _publish_eof(self):
        try:
            eof_message = Message(MessageType.EOF, None)
            self.connection.send(eof_message)
        except Exception as e:
            logger.error(f'Failed to publish EOF: {e}', exc_info=True)

    def process_message(self, message: Message):
        logger.info("Received message with results")
        logger.info(message.data)

    def shutdown(self):
        logger.info('Shutting down Cleaner...')
        self.is_running = False

        if self.client_socket:
            try:
                self.client_socket.close()
                logger.info('Client socket closed.')
            except Exception as e:
                logger.warning(f'Error closing client socket: {e}')
        if self.server_socket:
            try:
                self.server_socket.close()
                logger.info('Server socket closed.')
            except Exception as e:
                logger.warning(f'Error closing server socket: {e}')

        try:
            self.connection.close()
            logger.info('Connection closed.')
        except Exception as e:
            logger.error(f'Error closing Connection: {e}', exc_info=True)

        logger.info('Cleaner shutdown complete.')

    def process_results(self):
        try:
            logger.info("Begin to process results...")
            self.connection.recv(self.process_message)
        except Exception as e:
            logger.critical(f'Fatal error while obtaining results: {e}', exc_info=True)

    def run(self):
        if not self.is_running:
            logger.critical(
                'Cleaner configuration failed during initialization. Cannot run.'
            )
            return

        self._setup_signal_handlers()

        try:
            # if not self._connect_rabbitmq():
            #     return
            if not self._setup_server_socket():
                return

            if self._accept_client():
                logger.info('Beginning to receive movies...')
                self._process_client_data(BatchType.MOVIES)
                logger.info('Beginning to receive credits...')
                self._process_client_data(BatchType.CREDITS)
                logger.info('Beginning to receive ratings...')
                self._process_client_data(BatchType.RATINGS)

        except Exception as e:
            logger.critical(f'Fatal error during Cleaner execution: {e}', exc_info=True)
        finally:
            self.shutdown()


if __name__ == '__main__':
    cleaner_instance = None
    try:
        config = Config()
        cleaner_instance = Cleaner(config)
        cleaner_instance.run()
        cleaner_instance.process_results()
        logger.info('Cleaner run method finished.')
        sys.exit(0)
    except ValueError as e:
        logger.critical(f'Failed to initialize Cleaner: {e}')
        sys.exit(1)
    except Exception as e:
        logger.critical(
            f'Unhandled exception in main execution block: {e}', exc_info=True
        )
        if cleaner_instance:
            cleaner_instance.shutdown()
        sys.exit(1)
