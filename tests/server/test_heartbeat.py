import time
import unittest

from src.messaging.tcp_socket import TCPSocket
from src.server.heartbeat import Heartbeat


class TestHeartbeat:
    def test_heartbeat(self):
        heartbeat_port = 13434
        heartbeat = Heartbeat(heartbeat_port)
        watcher = None
        addr = ('127.0.0.1', heartbeat_port)
        tries = 0
        while tries < 3:
            try:
                watcher = TCPSocket.create_and_connect(addr)
                break
            except ConnectionRefusedError:
                tries += 1
                time.sleep(1)

        if watcher is None:
            heartbeat.stop()
            raise AssertionError

        message = b'H'
        bytes_amount = len(message)

        watcher.send(message, bytes_amount)

        expected_message = b'B'
        expected_bytes_amount = len(expected_message)
        recv_message = watcher.recv(expected_bytes_amount)
        watcher.stop()
        heartbeat.stop()

        assert recv_message == expected_message


if __name__ == '__main__':
    unittest.main()
