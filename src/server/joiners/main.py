import logging
import threading
import uuid
from typing import Dict, Type, Set

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
        # State
        self._base_loaded_events: Dict[uuid.UUID, threading.Event] = {}
        self.eofs_received: Set[uuid.UUID] = set()
        super().__init__(config, counter_type, has_data_persisted=True)

        logger.info(f"GenericJoinerNode '{counter_type}' initialized.")
        threading.Thread(target=self._thread_load_base, daemon=True).start()

    def _load_node_state(self):
        app_state = super()._load_node_state()
        if app_state is None:
            return
        base_data_state = app_state.get('base_data', {})
        if base_data_state and hasattr(self.logic, 'load_application_state'):
            self.logic.load_application_state(base_data_state)
            logger.info(
                'Application state (base_data) for Joiner restored successfully.'
            )

        eofs_str = app_state.get('eofs_received', [])
        self.eofs_received = {uuid.UUID(uid_str) for uid_str in eofs_str}
        for user_id in self.eofs_received:
            event = self._base_loaded_events.setdefault(user_id, threading.Event())
            event.set()
            logger.info(f'Restored EOF state for user {user_id}. Base data is ready.')

    def persist_changes(self, message: Message):
        # Don't need to delete duplicates, sink already does so.
        # Just need to persist when is going to merge
        # Also don't need to mind about duplicates when charging base_data because is already idempotent
        pass

    def _save_state(self):
        if not self.node_state_manager:
            return

        app_state = self.logic.get_application_state()

        state_to_persist = {
            'processed_message_ids': list(self.processed_message_ids),
            'application_state': {
                'base_data': app_state,
                'eofs_received': [str(uid) for uid in self.eofs_received],
            },
        }
        self.node_state_manager.save_state(state_to_persist)
        logger.debug('Joiner state saved successfully.')

    def _thread_load_base(self):
        queue_name = self.config.base_db[0]['queue']
        exchange = self.config.base_db[0]['exchange']
        logger.info(f'LOAD BASE DATA FROM QUEUE: {queue_name} and exchange {exchange}')
        broker = RabbitMQBroker(self.config.rabbit_host)
        consumer = BroadcastConsumer(broker, exchange, queue_name)

        def on_msg(msg: Message):
            user_id = msg.user_id
            if msg.message_type == MessageType.EOF:
                logger.info(f'BASE_DB EOF received for user {user_id}')
                self.logic.base_data.setdefault(user_id, {})
                self.eofs_received.add(user_id)
                self._save_state()

                event = self._base_loaded_events.setdefault(user_id, threading.Event())
                event.set()
            else:
                # chaos_test(0.1, f'Crash before saving. {msg.message_id}. Should be re-queued')
                # logger.info(f'Processing: {msg.message_id}')
                movies: list[Movie] = msg.data
                bucket = self.logic.base_data.setdefault(user_id, {})
                for movie in movies:
                    bucket[movie.id] = movie

                self._save_state()
                # logger.info(f'Saved {len(movies)} movies for user {user_id}')

        consumer.basic_consume(broker, on_msg)
        consumer.start_consuming(broker, on_msg)
        logger.info('Start consuming db for base data')

    def process_message(self, message: Message):
        event = self._base_loaded_events.setdefault(message.user_id, threading.Event())
        event.wait()
        super().process_message(message)

    def handle_message(self, message: Message):
        if not self.is_running():
            return
        try:
            joined_msg = self.logic.merge(message)

            if joined_msg and joined_msg.data:
                self.connection.send(joined_msg)
                # logger.info(f'Joined and sent {len(joined_msg.data)} items.')
                # chaos_test(0.001, f'Crash before processing {joined_msg.message_id}.'
                #                   f'Should be re-queued. This will generate duplicates in next stage')
                # logger.info(f'Processing: {joined_msg.message_id}')
                self.processed_message_ids.add(str(message.message_id))
                self._save_state()

        except Exception as e:
            logger.error(f'Error processing message in JoinerNode: {e}', exc_info=True)

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_JOINER_LOGICS


if __name__ == '__main__':
    GenericJoinerNode.main(AVAILABLE_JOINER_LOGICS)
