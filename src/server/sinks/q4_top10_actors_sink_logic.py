import logging

from src.messaging.protocol.message import Message, MessageType
from src.model.actor_count import ActorCount
from src.server.sinks.base_sink_logic import BaseSinkLogic
from src.utils.safe_dict import SafeDict

logger = logging.getLogger(__name__)


class Q4Top10ActorsSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.final_actor_count = SafeDict()
        logger.info('Q4Top10ActorsSinkLogic initialized.')

    def merge_results(self, message: Message):
        result_list: list[ActorCount] = message.data
        user_id = message.user_id
        partial_result: dict[str, ActorCount] = self.final_actor_count.get(user_id, {})

        for actor_count in result_list:
            counter = partial_result.get(actor_count.actor_name)

            if counter is None:
                counter = actor_count
            else:
                counter.count += actor_count.count

            partial_result[actor_count.actor_name] = counter

        self.final_actor_count.set(user_id, partial_result)

    def message_result(self, user_id: int) -> Message:
        sorted_top10_actors = self._obtain_sorted_top10_actors(user_id)
        logger.info(f'TOP 10: {sorted_top10_actors}')
        return Message(user_id, MessageType.ActorCount, sorted_top10_actors)

    def _obtain_sorted_top10_actors(self, user_id: int) -> list[ActorCount]:
        logger.info(f'--- Sink: Final Global TOP 10 Actors Counts for {user_id} ---')

        result: dict[str, ActorCount] = self.final_actor_count.pop(user_id, {})

        top10: list[ActorCount] = []

        if not result:
            logger.info('No actors count aggregated.')
            return top10

        try:
            sorted_top10_actors = sorted(
                result.items(),
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
