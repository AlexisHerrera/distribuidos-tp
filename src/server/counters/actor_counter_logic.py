import logging

from src.messaging.protocol.message import Message, MessageType

from ...model.actor_count import ActorCount
from .base_counter_logic import BaseCounterLogic

logger = logging.getLogger(__name__)


class ActorCounterLogic(BaseCounterLogic):
    def __init__(self):
        self.actor_counts: dict[int, dict[str, int]] = {}
        logger.info('ActorCounterLogic initialized.')

    def process_message(self, message: Message):
        actor_counts: list[ActorCount] = message.data
        user_id = message.user_id
        partial_result = self.actor_counts.get(user_id, {})

        for actor_count in actor_counts:
            partial_result[actor_count.actor_name] = (
                partial_result.get(actor_count.actor_name, 0) + actor_count.count
            )

        self.actor_counts[user_id] = partial_result

    def message_result(self, user_id: int) -> Message:
        final_result = []
        self.log_final_results(user_id)
        for name, count in self.actor_counts[user_id].items():
            final_result.append(ActorCount(name, count))

        self.actor_counts.pop(user_id, None)  # Drop silently
        return Message(user_id, MessageType.ActorCount, final_result)

    def log_final_results(self, user_id: int):
        logger.info('--- Final Actor Counts ---')
        if not self.actor_counts:
            logger.info('No actors count were counted.')
            return

        sorted_actors = sorted(
            self.actor_counts[user_id].items(), key=lambda item: item[1], reverse=True
        )

        logger.info('Top 5 Actors by Aparitions:')
        for i, (actor, aparitions) in enumerate(sorted_actors[:5]):
            logger.info(f'  {i + 1}. {actor}: {aparitions}')

        logger.info(f'Total actors counted: {len(sorted_actors)}')
        logger.info('-----------------------------')
