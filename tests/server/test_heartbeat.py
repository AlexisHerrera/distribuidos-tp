import unittest

from src.messaging.udp_socket import UDPSocket
from src.server.heartbeat import Heartbeat


class TestHeartbeat:
    class Config:
        def __init__(self):
            self.heartbeat_port = 13434
            self.heartbeat_watcher_port = 13435

    def test_heartbeat(self):
        config = TestHeartbeat.Config()
        heartbeat = Heartbeat(config)
        watcher = UDPSocket(config.heartbeat_watcher_port)

        message = b'H'
        bytes_amount = len(message)
        addr = ('localhost', config.heartbeat_port)

        watcher.sendto(addr, message, bytes_amount)

        expected_message = b'B'
        expected_bytes_amount = len(expected_message)
        recv_message, (host, port) = watcher.recvfrom(expected_bytes_amount)
        watcher.stop()
        heartbeat.stop()

        assert recv_message == expected_message
        assert host == 'localhost'
        assert port == 13434


if __name__ == '__main__':
    unittest.main()
