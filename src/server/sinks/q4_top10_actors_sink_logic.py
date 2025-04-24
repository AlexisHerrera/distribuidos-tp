import logging
from collections import defaultdict
from typing import Dict

from src.messaging.protocol.message import Message, MessageType
from src.model.actor_count import ActorCount
from src.server.sinks.base_sink_logic import BaseSinkLogic

logger = logging.getLogger(__name__)


class Q4Top10ActorsSinkLogic(BaseSinkLogic):
    def __init__(self):
        self.final_actor_count = defaultdict(int)
        logger.info('Q4Top10ActorsSinkLogic initialized.')

    def merge_results(self, message: Message):
        result_list: list[ActorCount] = message.data
        for actor_count in result_list:
            self.final_actor_count[actor_count.actor_name] += int(actor_count.count)

    def message_result(self) -> Message:
        sorted_top10_actors = self._obtain_sorted_top10_actors()
        logger.info(f'TOP 10: {sorted_top10_actors}')
        top_to_reporter = []
        for actor, count in sorted_top10_actors.items():
            top_to_reporter.append(ActorCount(actor, count))
        return Message(MessageType.ActorCount, top_to_reporter)

    def _obtain_sorted_top10_actors(self) -> Dict[str, int]:
        logger.info('--- Sink: Final Global TOP 10 Actors Counts ---')
        if not self.final_actor_count:
            logger.info('No actors count aggregated.')
            return {}

        try:
            sorted_top10_actors = sorted(
                self.final_actor_count.items(), key=lambda item: item[1], reverse=True
            )
            logger.info('FINAL Top 10 Actors:')
            for i, (actor, count) in enumerate(sorted_top10_actors[:10]):
                logger.info(f'  {i + 1}. {actor}: {count}')
            logger.info(f'Total actors aggregated: {len(sorted_top10_actors)}')
            logger.info('---------------------------------------------')

            return dict(sorted_top10_actors[:10])
        except Exception as e:
            logger.error(f'Error logging final results: {e}', exc_info=True)
            return None
