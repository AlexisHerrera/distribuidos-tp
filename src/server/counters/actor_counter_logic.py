import logging
from collections import defaultdict

from src.messaging.protocol.message import Message, MessageType

from ...model.actor_count import ActorCount
from .base_counter_logic import BaseCounterLogic

logger = logging.getLogger(__name__)


class ActorCounterLogic(BaseCounterLogic):
    def __init__(self):
        self.actor_counts = defaultdict(int)
        logger.info('ActorCounterLogic initialized.')

    def process_message(self, message: Message):
        actor_counts: list[ActorCount] = message.data
        for actor_count in actor_counts:
            self.actor_counts[actor_count.actor_name] = (
                self.actor_counts.get(actor_count.actor_name, 0) + actor_count.count
            )

    def message_result(self, user_id: int) -> Message:
        # user_id = 1  # TODO: `user_id` will probably be part of `self.actor_counts` and/or needs to be passed as parameter
        final_result = []
        self.log_final_results()
        for name, count in self.actor_counts.items():
            final_result.append(ActorCount(name, count))
        return Message(user_id, MessageType.ActorCount, final_result)

    def log_final_results(self):
        logger.info('--- Final Actor Counts ---')
        if not self.actor_counts:
            logger.info('No actors count were counted.')
            return

        sorted_actors = sorted(
            self.actor_counts.items(), key=lambda item: item[1], reverse=True
        )

        logger.info('Top 5 Actors by Aparitions:')
        for i, (actor, aparitions) in enumerate(sorted_actors[:5]):
            logger.info(f'  {i + 1}. {actor}: {aparitions}')

        logger.info(f'Total actors counted: {len(sorted_actors)}')
        logger.info('-----------------------------')
