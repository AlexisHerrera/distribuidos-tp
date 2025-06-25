import logging
import uuid
from typing import Any

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

    def message_result(self, user_id: uuid.UUID) -> Message:
        sorted_top10_actors = self._obtain_sorted_top10_actors(user_id)
        logger.info(f'TOP 10: {sorted_top10_actors}')
        return Message(
            user_id, MessageType.ActorCount, sorted_top10_actors, message_id=None
        )

    def _obtain_sorted_top10_actors(self, user_id: uuid.UUID) -> list[ActorCount]:
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

    def get_application_state(self) -> dict[str, Any]:
        serializable_state = {}
        for user_id, actor_dict in self.final_actor_count.to_dict().items():
            serializable_state[str(user_id)] = {
                actor_name: counter.to_dict()
                for actor_name, counter in actor_dict.items()
            }
        return serializable_state

    def load_application_state(self, state: dict[str, Any]) -> None:
        logger.info('Loading application state for Q4Top10ActorsSinkLogic...')
        deserialized_state = {}
        for user_id, actor_dict in state.items():
            deserialized_state[uuid.UUID(user_id)] = {
                actor_name: ActorCount.from_dict(counter_dict)
                for actor_name, counter_dict in actor_dict.items()
            }
        self.final_actor_count = SafeDict(initial_dict=deserialized_state)
