import unittest
import uuid

from src.messaging.protocol.message import Message, MessageType
from src.model.actor_count import ActorCount
from src.server.counters.actor_counter_logic import ActorCounterLogic


class TestActorCounterLogic(unittest.TestCase):
    def test_process_batch_aggregates_correctly(self):
        """
        Verify that a single message (batch) with multiple actor counts
        is aggregated correctly and returned immediately.
        """
        logic = ActorCounterLogic()
        user_id = uuid.uuid4()

        input_actors = [
            ActorCount('Ricardo Darin', 1),
            ActorCount('Guillermo Francella', 1),
            ActorCount('Ricardo Darin', 1),
            ActorCount('Soledad Villamil', 1),
            ActorCount('Ricardo Darin', 1),
        ]
        input_message = Message(user_id, MessageType.ActorCount, input_actors)

        output_message = logic.process_message(input_message)

        self.assertIsNotNone(
            output_message, 'The method should return a Message, not None.'
        )
        self.assertEqual(user_id, output_message.user_id)
        self.assertEqual(MessageType.ActorCount, output_message.message_type)

        actual_counts = {actor.actor_name: actor.count for actor in output_message.data}

        expected_counts = {
            'Ricardo Darin': 3,
            'Guillermo Francella': 1,
            'Soledad Villamil': 1,
        }

        self.assertEqual(expected_counts, actual_counts)

    def test_process_empty_batch_returns_none(self):
        """
        Verify that processing a message with no data returns None.
        """
        logic = ActorCounterLogic()
        user_id = uuid.uuid4()
        input_message = Message(user_id, MessageType.ActorCount, [])

        output_message = logic.process_message(input_message)

        self.assertIsNone(
            output_message, 'Processing an empty list should return None.'
        )


if __name__ == '__main__':
    unittest.main()
