import threading
import unittest

from src.messaging.udp_socket import UDPSocket


class TestUDPSocket:
    def test_udpsocket_send_and_recv_message(self):
        server_port = 12345
        client_port = 12346
        server = UDPSocket(server_port)
        client = UDPSocket(client_port)

        server_addr = ('127.0.0.1', server_port)
        client_addr = ('127.0.0.1', client_port)
        message = b'H'
        bytes_amount = len(message)
        client.sendto(server_addr, message, bytes_amount)

        (recv_message, addr) = server.recvfrom(bytes_amount)

        server.stop()
        client.stop()

        assert recv_message == message
        assert addr == client_addr

    def test_udpsocket_recv_after_close(self):
        server_port = 12345
        server = UDPSocket(server_port)

        # server_addr = ('127.0.0.1', server_port)
        message = b'H'
        bytes_amount = len(message)

        def test_recv():
            (recv_message, addr) = server.recvfrom(bytes_amount)
            assert recv_message is None
            assert addr is None

        t = threading.Thread(target=test_recv)
        t.start()

        server.stop()

        t.join()


if __name__ == '__main__':
    unittest.main()
