import unittest
from unittest.mock import Mock

from src.server.watcher.bully.state_manager import BullyState, BullyStateManager


class TestStateManager:
    def test_election_when_in_running_calls_init_election(self):
        state_manager = BullyStateManager(BullyState.RUNNING, 1)
        init_election = Mock()

        state_manager.election(init_election)

        init_election.assert_called_once()

        assert state_manager.is_in_state(BullyState.ELECTION)

    def test_election_when_in_election_no_calls(self):
        state_manager = BullyStateManager(BullyState.ELECTION, 1)
        init_election = Mock()

        state_manager.election(init_election)

        init_election.assert_not_called()

        assert state_manager.is_in_state(BullyState.ELECTION)

    def test_election_when_in_waiting_coordinator_no_calls(self):
        state_manager = BullyStateManager(BullyState.WAITING_COORDINATOR, 1)
        init_election = Mock()

        state_manager.election(init_election)

        init_election.assert_not_called()

        assert state_manager.is_in_state(BullyState.WAITING_COORDINATOR)

    def test_answer_when_in_election_no_calls(self):
        state_manager = BullyStateManager(BullyState.ELECTION, 1)

        state_manager.answer()

        assert state_manager.is_in_state(BullyState.WAITING_COORDINATOR)

    def test_answer_when_in_waiting_coordinator_no_calls(self):
        state_manager = BullyStateManager(BullyState.WAITING_COORDINATOR, 1)

        state_manager.answer()

        assert state_manager.is_in_state(BullyState.WAITING_COORDINATOR)

    def test_answer_when_in_running_no_calls(self):
        state_manager = BullyStateManager(BullyState.RUNNING, 1)

        state_manager.answer()

        assert state_manager.is_in_state(BullyState.RUNNING)

    def test_coordinator_when_in_running_higher_node_id_sets_new_leader(self):
        node_id = 1
        new_leader_node_id = 3
        state_manager = BullyStateManager(BullyState.RUNNING, node_id)

        set_leader = Mock()
        send_coordinator = Mock()

        state_manager.coordinator(new_leader_node_id, set_leader, send_coordinator)

        set_leader.assert_called_once()
        send_coordinator.assert_not_called()

        assert state_manager.is_in_state(BullyState.RUNNING)

    def test_coordinator_when_in_running_new_leader_lower_id(self):
        node_id = 3
        new_leader_node_id = 1
        state_manager = BullyStateManager(BullyState.RUNNING, node_id)

        set_leader = Mock()
        send_coordinator = Mock()

        state_manager.coordinator(new_leader_node_id, set_leader, send_coordinator)

        set_leader.assert_not_called()
        send_coordinator.assert_called_once()


if __name__ == '__main__':
    unittest.main()
