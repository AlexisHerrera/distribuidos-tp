import logging

from src.messaging.protocol.message import Message, MessageType
from src.utils.safe_dict import SafeDict

from ...model.actor_count import ActorCount
from .base_counter_logic import BaseCounterLogic

logger = logging.getLogger(__name__)


class ActorCounterLogic(BaseCounterLogic):
    def __init__(self):
        self.actor_counts = SafeDict()
        logger.info('ActorCounterLogic initialized.')

    def process_message(self, message: Message) -> Message | None:
        actor_counts_list: list[ActorCount] = message.data
        user_id = message.user_id

        batch_result: dict[str, int] = {}

        for actor_count in actor_counts_list:
            batch_result[actor_count.actor_name] = (
                batch_result.get(actor_count.actor_name, 0) + actor_count.count
            )

        if not batch_result:
            return None

        result_list = [ActorCount(name, count) for name, count in batch_result.items()]

        logger.info(f'[{user_id}] Sending {len(result_list)} partial actor counts.')

        return Message(user_id, MessageType.ActorCount, result_list)
