import unittest

from src.utils.util import rotate


class TestUtil:
    def test_rotate_list_node_1(self):
        node_id = 1
        expected = [2, 3, 4]
        peer_list = [2, 3, 4]

        result = rotate(peer_list, node_id - 1)

        assert len(result) == len(expected)

        for i in range(len(expected)):
            assert result[i] == expected[i]

    def test_rotate_list_node_2(self):
        node_id = 2
        expected = [3, 4, 1]
        peer_list = [1, 3, 4]

        result = rotate(peer_list, node_id - 1)

        assert len(result) == len(expected)

        for i in range(len(expected)):
            assert result[i] == expected[i]

    def test_rotate_list_node_3(self):
        node_id = 3
        expected = [4, 1, 2]
        peer_list = [1, 2, 4]

        result = rotate(peer_list, node_id - 1)

        assert len(result) == len(expected)

        for i in range(len(expected)):
            assert result[i] == expected[i]

    def test_rotate_list_node_4(self):
        node_id = 4
        expected = [1, 2, 3]
        peer_list = [1, 2, 3]

        result = rotate(peer_list, node_id - 1)

        assert len(result) == len(expected)

        for i in range(len(expected)):
            assert result[i] == expected[i]

    def test_rotate_list_node_of_len_1(self):
        node_id = 1
        expected = [2]
        peer_list = [2]

        result = rotate(peer_list, node_id - 1)

        assert len(result) == len(expected)

        for i in range(len(expected)):
            assert result[i] == expected[i]


if __name__ == '__main__':
    unittest.main()
