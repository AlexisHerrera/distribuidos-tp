import logging
import os
import signal
import sys
from collections import defaultdict

from src.common.protocol.batch import Batch, BatchType
from src.common.socket_communication import (
    receive_message,
    accept_new_connection,
    create_server_socket,
)

from src.messaging.broker import RabbitMQBroker
from src.messaging.protocol.message import Message, MessageType
from src.model.movie import Movie
from src.model.cast import Cast
from src.server.cleaner.clean_movies import parse_line_to_movie
from src.server.cleaner.clean_credits import parse_line_to_credits
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
        self.broker: RabbitMQBroker | None = None
        self.server_socket = None
        self.client_socket = None

        try:
            self.port = int(os.getenv('SERVER_PORT', '12345'))
            self.backlog = int(os.getenv('LISTENING_BACKLOG', '3'))
            self.rabbit_host = self.config.rabbit_host
            self.output_queue_movies = self.config.get_env_var(
                'MOVIES_CLEANED_QUEUE', 'movies_cleaned_queue'
            )
            self.output_queue_credits = self.config.get_env_var(
                'CREDITS_CLEANED_QUEUE', 'credits_cleaned_queue'
            )

            if not self.rabbit_host or not self.output_queue_movies:
                raise ValueError(
                    'Missing essential configuration: RABBIT_HOST or OUTPUT_QUEUE'
                )

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

    def _connect_rabbitmq(self):
        if not self.is_running:
            return False
        try:
            logger.info(f'Connecting to RabbitMQ at {self.rabbit_host}...')
            self.broker = RabbitMQBroker(self.rabbit_host)
            logger.info('Connected to RabbitMQ.')
            logger.info(f'Declaring output queue: {self.output_queue_movies}')
            self.broker.queue_declare(
                queue_name=self.output_queue_movies, exclusive=False, durable=True
            )
            return True
        except Exception as e:
            logger.critical(f'Failed to connect/configure RabbitMQ: {e}', exc_info=True)
            self.is_running = False
            return False

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
        # elif batch.type == BatchType.RATINGS:
        #     return self._batch_data_to_ratings(batch.data)
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

    def _process_client_data(self, type_of_data):
        if not self.is_running or not self.client_socket or not self.broker:
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
                    if type_of_data == BatchType.MOVIES:
                        self._publish_eof(self.output_queue_movies)
                    elif type_of_data == BatchType.CREDITS:
                        self._publish_eof(self.output_queue_credits)
                    # self._publish_eof(self.output_queue_ratings)
                    break

                object_list = self.batch_to_list_objects(batch)

                target_queue = None
                message_type_enum = None
                if batch.type == BatchType.MOVIES:
                    target_queue = self.output_queue_movies
                    message_type_enum = MessageType.Movie
                elif batch.type == BatchType.CREDITS:
                    target_queue = self.output_queue_credits
                    message_type_enum = MessageType.Cast
                # elif batch.type == BatchType.RATINGS:
                #     target_queue = self.output_queue_ratings
                #     message_type_enum = MessageType.Rating

                if not target_queue or not message_type_enum:
                    logger.warning(
                        f'No target queue or message type defined for BatchType: {batch.type.name}. Skipping batch.'
                    )
                    continue

                count_in_batch = 0
                for model_object in object_list:
                    if not self.is_running:
                        break
                    try:
                        output_message = Message(message_type_enum, [model_object])
                        self.broker.put(
                            routing_key=target_queue, body=output_message.to_bytes()
                        )
                        count_in_batch += 1
                    except Exception as e:
                        obj_id = getattr(model_object, 'id', 'N/A')
                        logger.error(
                            f"Error publishing {message_type_enum.name} ID {obj_id} to '{target_queue}': {e}",
                            exc_info=True,
                        )

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
                self.is_running = False

        logger.info('Client data processing finished.')
        logger.info(f'Processing Summary: {dict(processed_counts)}')

    def _publish_eof(self, target_queue: str):
        if self.broker and target_queue:
            try:
                eof_message = Message(MessageType.EOF, None)
                self.broker.put(routing_key=target_queue, body=eof_message.to_bytes())
                logger.info(f"EOF message published to '{target_queue}'")
            except Exception as e:
                logger.error(
                    f"Failed to publish EOF to '{target_queue}': {e}", exc_info=True
                )

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

        if self.broker:
            try:
                self.broker.close()
                logger.info('RabbitMQ broker connection closed.')
            except Exception as e:
                logger.error(
                    f'Error closing RabbitMQ broker connection: {e}', exc_info=True
                )
        logger.info('Cleaner shutdown complete.')

    def run(self):
        if not self.is_running:
            logger.critical(
                'Cleaner configuration failed during initialization. Cannot run.'
            )
            return

        self._setup_signal_handlers()

        try:
            if not self._connect_rabbitmq():
                return
            if not self._setup_server_socket():
                return

            if self._accept_client():
                logger.info('Beginning to receive movies...')
                self._process_client_data(BatchType.MOVIES)
                logger.info('Beginning to receive credits...')
                self._process_client_data(BatchType.CREDITS)

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
