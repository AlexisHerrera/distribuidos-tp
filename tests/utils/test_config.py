import os
import unittest

from src.utils.config import Config, WatcherConfig


class TestConfig:
    def test_read_yaml_file(self):
        path = os.getcwd() + '/tests/utils/'
        config = Config(path + 'test.yaml')

        assert config.rabbit_host == 'rabbit'
        assert len(config.consumers) == 1
        assert config.consumers[0]['type'] == 'broadcast'
        assert config.consumers[0]['queue'] == 'some_queue'
        assert len(config.publishers) == 1
        assert config.publishers[0]['type'] == 'direct'
        assert config.publishers[0]['queue'] == 'another_queue'
        assert config.log_level == 'ERROR'

    def test_read_yaml_multiple_publishers(self):
        path = os.getcwd() + '/tests/utils/'
        config = Config(path + 'test_multiple.yaml')

        assert config.rabbit_host == 'rabbit'
        assert len(config.consumers) == 1
        assert config.consumers[0]['type'] == 'broadcast'
        assert config.consumers[0]['queue'] == 'some_queue'
        assert len(config.publishers) == 2
        assert config.publishers[0]['type'] == 'direct'
        assert config.publishers[0]['queue'] == 'another_queue'
        assert config.publishers[1]['type'] == 'broadcast'
        assert config.publishers[1]['queue'] == 'other_queue'

    class TestWatcherConfig:
        def test_read_env_var_multiple_peers(self):
            expected = {'watcher-1': 1, 'watcher-2': 2, 'watcher-3': 3}
            os.environ['NODE_ID'] = '1'
            os.environ['PEERS'] = '1:watcher-1,2:watcher-2,3:watcher-3'
            path = os.getcwd() + '/tests/utils/'
            config = WatcherConfig(path + 'test_watcher.yaml')

            assert len(config.peers) == len(expected)
            for k, v in config.peers.items():
                assert k in expected
                assert v == expected[k]

        def test_read_env_var_one_peer(self):
            expected = {'watcher-1': 1}
            os.environ['NODE_ID'] = '1'
            os.environ['PEERS'] = '1:watcher-1'
            path = os.getcwd() + '/tests/utils/'
            config = WatcherConfig(path + 'test_watcher.yaml')

            assert len(config.peers) == len(expected)
            for k, v in config.peers.items():
                assert k in expected
                assert v == expected[k]

        def test_read_env_var_no_peers(self):
            expected = {}
            os.environ['NODE_ID'] = '1'
            os.environ['PEERS'] = ''
            path = os.getcwd() + '/tests/utils/'
            config = WatcherConfig(path + 'test_watcher.yaml')

            assert len(config.peers) == len(expected)
            assert config.peers == expected

        def test_read_env_is_empty(self):
            expected = {}
            os.environ['NODE_ID'] = '1'
            path = os.getcwd() + '/tests/utils/'
            config = WatcherConfig(path + 'test_watcher.yaml')

            assert len(config.peers) == len(expected)
            assert config.peers == expected


if __name__ == '__main__':
    unittest.main()
