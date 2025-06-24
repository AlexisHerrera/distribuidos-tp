import logging
import random
import signal
import subprocess
from queue import Empty, SimpleQueue
from threading import Lock

from src.utils.config import ChaosMonkeyConfig
from src.utils.log import initialize_log

logger = logging.getLogger(__name__)

EXIT_QUEUE = b'q'

MINIMUM_STOP_SERVICES = 1


class ChaosMonkey:
    def __init__(self, config: ChaosMonkeyConfig):
        self.max_stop = max(config.max_stop, MINIMUM_STOP_SERVICES)
        self.frequency = config.frequency
        self.nodes = config.nodes

        self.is_running_lock = Lock()
        self.is_running = True
        self.exit_queue: SimpleQueue = SimpleQueue()
        self._setup_signal_handlers()
        logger.info(
            f'[CHAOS_MONKEY] Initialized with frequency {self.frequency} and max stop {self.max_stop}'
        )

    def _setup_signal_handlers(self):
        def signal_handler(signum, _frame):
            logger.warning(
                f'Signal {signal.Signals(signum).name} received. Initiating shutdown...'
            )
            self.exit_queue.put(EXIT_QUEUE)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info('Signal handlers configured.')

    def run(self):
        while self._is_running():
            nodes = random.choices(
                self.nodes, k=random.randint(MINIMUM_STOP_SERVICES, self.max_stop)
            )

            for node in nodes:
                logger.info(f'[CHAOS_MONKEY] Stopping node {node}')
                self._stop_service(node)

            try:
                logger.info('[CHAOS_MONKEY] Sleeping...')
                self.exit_queue.get(timeout=self.frequency)
                break
            except Empty:
                continue

    def _stop_service(self, node_name: str):
        result = subprocess.run(
            ['docker', 'kill', '--signal=SIGKILL', node_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info(
            'Command executed. Result={}. Output={}. Error={}'.format(
                result.returncode, result.stdout, result.stderr
            )
        )

    def _is_running(self) -> bool:
        with self.is_running_lock:
            return self.is_running

    def stop(self):
        logger.info('[CHAOS_MONKEY] Stopping')
        with self.is_running_lock:
            self.is_running = False

        self.exit_queue.put(EXIT_QUEUE)

        logger.info('[CHAOS_MONKEY] Stopped')


def main():
    config = ChaosMonkeyConfig()
    initialize_log(config.log_level)
    monkey = ChaosMonkey(config)

    try:
        monkey.run()
    except Exception as e:
        logger.error(f'[CHAOS_MONKEY] Error: {e}')
    finally:
        monkey.stop()


if __name__ == '__main__':
    main()
