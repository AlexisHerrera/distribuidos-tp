import logging
import threading
from typing import Dict, Type, Any

from src.messaging.broker import RabbitMQBroker
from src.messaging.consumer import BroadcastConsumer
from src.messaging.protocol.message import Message, MessageType
from src.model.movie import Movie
from src.server.base_node import BaseNode
from src.server.joiners.base_joiner_logic import BaseJoinerLogic
from src.server.joiners.cast_joiner_logic import CastJoinerLogic
from src.server.joiners.ratings_joiner_logic import RatingsJoinerLogic
from src.utils.config import Config

logger = logging.getLogger(__name__)

# Registry of available counter types
AVAILABLE_JOINER_LOGICS: Dict[str, Type[BaseJoinerLogic]] = {
    'ratings_joiner': RatingsJoinerLogic,
    'cast_joiner': CastJoinerLogic,
}


class GenericJoinerNode(BaseNode):
    def __init__(self, config: Config, counter_type: str):
        super().__init__(config, counter_type)
        logger.info(f"GenericJoinerNode '{counter_type}' initialized.")
        self._base_loaded = threading.Event()
        threading.Thread(target=self._thread_load_base, daemon=True).start()

    def _thread_load_base(self):
        queue_name = self.config.base_db[0]['queue']
        exchange = self.config.base_db[0]['exchange']
        logger.info(f'LOAD BASE DATA FROM QUEUE: {queue_name} and exchange {exchange}')
        broker = RabbitMQBroker(self.config.rabbit_host)
        consumer = BroadcastConsumer(broker, exchange, queue_name)
        data: dict[int, Any] = {}

        def on_msg(msg: Message):
            if msg.message_type == MessageType.EOF:
                logger.info('BASE_DB EOF received')
                broker.basic_cancel(consumer.tag)
                broker.close()
                self.logic.base_data[msg.user_id] = data
                self._base_loaded.set()
            else:
                movies: list[Movie] = msg.data
                for movie in movies:
                    data[movie.id] = movie
                logger.info(f'saved {len(movies)}')

        consumer.basic_consume(broker, on_msg)
        consumer.start_consuming(broker, on_msg)
        logger.info('Start consuming db')

    def process_message(self, message: Message):
        self._base_loaded.wait()
        super().process_message(message)

    def handle_message(self, message: Message):
        if not self.is_running():
            return
        try:
            joined = self.logic.merge(message)
            self.connection.send(joined)
            # logger.info(f'Joined {joined}')
        except Exception as e:
            logger.error(f'Error processing message in JoinerNode: {e}', exc_info=True)

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_JOINER_LOGICS


if __name__ == '__main__':
    GenericJoinerNode.main(AVAILABLE_JOINER_LOGICS)
