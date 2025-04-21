import os
import unittest

from src.utils.config import Config


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


if __name__ == '__main__':
    unittest.main()
