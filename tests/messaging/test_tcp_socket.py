import queue
import socket
import threading
import time
import unittest

import pytest

from src.messaging.tcp_socket import TCPSocket


@pytest.mark.skip(reason='May throw warning with address already in use')
class TestTCPSocket:
    def test_tcpsocket_send_and_recv_message(self):
        server_port = 12345
        server_addr = ('127.0.0.1', server_port)
        sent_message = b'H'
        server = TCPSocket.create()

        def run_server():
            server.bind(server_addr)
            server.listen(1)
            client_accepted, _addr = server.accept()
            message = client_accepted.recv(1)

            assert message == sent_message
            try:
                client_accepted.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                client_accepted.close()
            except OSError:
                pass

        t = threading.Thread(target=run_server)
        t.start()

        tries = 0
        while tries < 3:
            try:
                client = TCPSocket.create_and_connect(server_addr)
                bytes_amount = len(sent_message)
                client.send(sent_message, bytes_amount)

                client.stop()
                break
            except ConnectionRefusedError:
                tries += 1
                time.sleep(1)  # Wait for server to accept connections

        server.stop()
        t.join()

    def test_tcpsocket_recv_after_close(self):
        server_port = 12349
        server_addr = ('127.0.0.1', server_port)
        server = TCPSocket.create()
        q = queue.SimpleQueue()

        def run_server():
            server.bind(server_addr)
            server.listen(1)
            q.put(True)
            with pytest.raises(OSError):
                _client_accepted, _addr = server.accept()

        t = threading.Thread(target=run_server)
        t.start()

        # Wait for server to bind and listen
        _ = q.get()
        server.stop()
        t.join()


if __name__ == '__main__':
    unittest.main()
