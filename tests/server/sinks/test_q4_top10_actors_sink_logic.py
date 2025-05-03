import unittest

from src.messaging.protocol.message import Message, MessageType
from src.model.actor_count import ActorCount
from src.server.sinks.q4_top10_actors_sink_logic import Q4Top10ActorsSinkLogic


class TestQ4Top10ActorsSinkLogic:
    def test_sort_by_count_success(self):
        q4 = Q4Top10ActorsSinkLogic()

        actor_counts = []
        actor_names = ['a', 'b', 'c', 'd', 'e', 'f']

        for i in range(len(actor_names)):
            actor_counts.append(ActorCount(actor_names[i], i))

        q4.merge_results(Message(1, MessageType.ActorCount, actor_counts))

        result = q4.message_result(1).data

        # Sort reverse by actor name as count goes up
        # ('a', 1), ('b', 2), ('c', 3), etc...
        actors_sorted = sorted(actor_counts, key=lambda x: x.actor_name, reverse=True)

        assert len(result) == len(actor_names)
        for i, r in enumerate(result):
            assert r.actor_name == actors_sorted[i].actor_name

    def test_sort_by_count_and_name_success(self):
        q4 = Q4Top10ActorsSinkLogic()

        actor_counts = [
            ActorCount('Ricardo Darín', 4),
            ActorCount('Valeria Bertucelli', 4),
            ActorCount('Leonardo Sbaraglia', 4),
            ActorCount('Alejandro Awada', 4),
        ]

        q4.merge_results(Message(1, MessageType.ActorCount, actor_counts))

        result = q4.message_result(1).data

        # Sort by actor name as count is equal for all
        actors_sorted = sorted(actor_counts, key=lambda x: x.actor_name, reverse=False)

        assert len(result) == len(actor_counts)
        for i, r in enumerate(result):
            assert r.actor_name == actors_sorted[i].actor_name


if __name__ == '__main__':
    unittest.main()
