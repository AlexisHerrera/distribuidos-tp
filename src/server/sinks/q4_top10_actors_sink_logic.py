import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.actor_count import ActorCount
from src.server.sinks.base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q4Top10ActorsSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.final_actor_count: dict[str, ActorCount] = {}
        logger.info('Q4Top10ActorsSinkLogic initialized.')

    def merge_results(self, message: Message):
        result_list: list[ActorCount] = message.data
        for actor_count in result_list:
            counter = self.final_actor_count.get(actor_count.actor_name)

            if counter is None:
                counter = actor_count
            else:
                counter.count += actor_count.count

            self.final_actor_count[actor_count.actor_name] = counter

    def message_result(self, user_id: int) -> Message:
        sorted_top10_actors = self._obtain_sorted_top10_actors()
        logger.info(f'TOP 10: {sorted_top10_actors}')
        return Message(user_id, MessageType.ActorCount, sorted_top10_actors)

    def _obtain_sorted_top10_actors(self) -> list[ActorCount]:
        logger.info('--- Sink: Final Global TOP 10 Actors Counts ---')

        top10: list[ActorCount] = []

        if not self.final_actor_count:
            logger.info('No actors count aggregated.')
            return top10

        try:
            sorted_top10_actors = sorted(
                self.final_actor_count.items(),
                key=lambda item: (-item[1].count, item[1].actor_name),
            )

            top10 = [v for (_, v) in sorted_top10_actors[:10]]

            logger.info('FINAL Top 10 Actors:')

            for i, actor_counter in enumerate(top10):
                actor = actor_counter.actor_name
                count = actor_counter.count
                logger.info(f'  {i + 1}. {actor}: {count}')
            logger.info(f'Total actors aggregated: {len(sorted_top10_actors)}')
            logger.info('---------------------------------------------')

            return top10
        except Exception as e:
            logger.error(f'Error logging final results: {e}', exc_info=True)
            return None
