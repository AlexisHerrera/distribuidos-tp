import threading
import time
import unittest
import uuid
from queue import SimpleQueue

from src.utils.eof_tracker import EOFTracker


class TestEOFTracker:
    def test_wait_user_to_process_synchronized_1(self):
        tracker = EOFTracker()
        user_id = uuid.uuid4()
        q = SimpleQueue()

        def process_user():
            tracker.processing(user_id)
            q.put(True)
            tracker.done(user_id)

        t = threading.Thread(target=process_user)
        t.start()

        _ = q.get()
        tracker.wait(user_id)
        t.join()

    def test_wait_user_to_process_synchronized_2(self):
        tracker = EOFTracker()
        user_id = uuid.uuid4()
        q = SimpleQueue()

        def wait_user():
            q.put(True)
            tracker.wait(user_id)

        t = threading.Thread(target=wait_user)
        t.start()

        _ = q.get()
        tracker.processing(user_id)
        time.sleep(1)
        tracker.done(user_id)

        t.join()

    def test_process_another_user_returns_automatically(self):
        tracker = EOFTracker()
        user_1_id = uuid.uuid4()
        user_2_id = uuid.uuid4()
        q = SimpleQueue()

        def process_user():
            tracker.processing(user_1_id)
            q.put(True)
            tracker.done(user_1_id)

        t = threading.Thread(target=process_user)
        t.start()

        _ = q.get()
        tracker.wait(user_2_id)
        t.join()

    def test_not_processing_user(self):
        tracker = EOFTracker()
        user_id = uuid.uuid4()

        tracker.wait(user_id)

    # def test_processing_user_fails_once(self):
    #     tracker = EOFTracker()
    #     user_id = uuid.uuid4()
    #     q = SimpleQueue()
    #
    #     def process_user():
    #         ok = False
    #         tracker.processing(user_id)
    #         q.put(1)
    #         tracker.done(user_id, ok)
    #
    #         ok = True
    #         tracker.processing(user_id)
    #         tracker.done(user_id, ok)
    #         q.put(2)
    #
    #     def wait_user():
    #         assert q.get() == 1
    #         tracker.wait(user_id)
    #         assert q.get() == 2
    #
    #     t_process = threading.Thread(target=process_user)
    #     t_wait = threading.Thread(target=wait_user)
    #     t_process.start()
    #     t_wait.start()
    #
    #     q.put(3)
    #
    #     t_process.join()
    #     t_wait.join()
    #


if __name__ == '__main__':
    unittest.main()
