import unittest

from src.messaging.protocol.message import Message, MessageType
from src.model.actor_count import ActorCount
from src.server.counters.actor_counter_logic import ActorCounterLogic


class TestActorCounterSinkLogic:
    def test_group_by_user_id_success(self):
        controller = ActorCounterLogic()

        actor_counts_1 = []
        actor_names = ['a', 'b', 'c', 'd', 'e', 'f']

        for i in range(len(actor_names)):
            actor_counts_1.append(ActorCount(actor_names[i], i))

        user_id = 1

        controller.process_message(
            Message(user_id, MessageType.ActorCount, actor_counts_1)
        )

        result = controller.message_result(user_id)

        assert result.user_id == user_id
        assert len(result.data) == len(actor_counts_1)

        actor_counts_2 = []
        reduced_len = len(actor_names) // 2

        for i in range(len(actor_names[:reduced_len])):
            actor_counts_2.append(ActorCount(actor_names[i], i))

        user_id = 2

        controller.process_message(
            Message(user_id, MessageType.ActorCount, actor_counts_2)
        )

        result = controller.message_result(user_id)

        assert result.user_id == user_id
        assert len(result.data) == len(actor_counts_2)


if __name__ == '__main__':
    unittest.main()
