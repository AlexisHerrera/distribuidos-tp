from threading import Event


class SleepInterrupted(Exception):
    def __init__(self):
        super().__init__('Sleep interrupted')


class CancellableSleep:
    def __init__(self):
        self.event = Event()

    def __call__(self, secs: int):
        if not self.event.wait(secs):
            return
        raise SleepInterrupted

    def cancel(self):
        self.event.set()
