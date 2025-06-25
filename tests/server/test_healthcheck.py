import time
import unittest

from src.messaging.protocol.healthcheck import HealthcheckProtocol
from src.messaging.tcp_socket import TCPSocket
from src.server.healthcheck import Healthcheck


class TestHealthcheck:
    def test_healthcheck(self):
        healthcheck_port = 13434
        healthcheck = Healthcheck(healthcheck_port)
        watcher = None
        addr = ('127.0.0.1', healthcheck_port)
        tries = 0
        while tries < 3:
            try:
                watcher = TCPSocket.create_and_connect(addr)
                break
            except ConnectionRefusedError:
                tries += 1
                time.sleep(1)

        if watcher is None:
            healthcheck.stop()
            raise AssertionError

        message = HealthcheckProtocol.PING
        bytes_amount = HealthcheckProtocol.MESSAGE_BYTES_AMOUNT

        watcher.send(message, bytes_amount)

        expected_message = HealthcheckProtocol.PONG
        expected_bytes_amount = HealthcheckProtocol.MESSAGE_BYTES_AMOUNT
        recv_message = watcher.recv(expected_bytes_amount)
        watcher.stop()
        healthcheck.stop()

        assert recv_message == expected_message


if __name__ == '__main__':
    unittest.main()
