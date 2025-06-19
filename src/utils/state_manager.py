import json
import logging
import os
import threading
from typing import Dict

STATE_DIR = '/app/state'
STATE_FILE = os.path.join(STATE_DIR, 'state.json')

logger = logging.getLogger(__name__)


class StateManager:
    def __init__(self):
        self._lock = threading.Lock()
        if not os.path.exists(STATE_DIR):
            os.makedirs(STATE_DIR)

        if not os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'w') as f:
                json.dump({}, f)

    def load_state(self) -> Dict:
        with self._lock:
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                logger.error(f'Error loading state file: {e}. Returning empty state.')
                return {}

    def save_state(self, state: Dict):
        with self._lock:
            try:
                with open(STATE_FILE, 'w') as f:
                    json.dump(state, f, indent=2)
            except IOError as e:
                logger.error(f'Error saving state file: {e}')
