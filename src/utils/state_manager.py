import errno
import json
import logging
import os
import threading
import uuid
from typing import Dict, Any

STATE_DIR = '/app/state'
STATE_FILE = os.path.join(STATE_DIR, 'state.json')

logger = logging.getLogger(__name__)


class StateManager:
    def __init__(self):
        self._lock = threading.Lock()
        if not os.path.exists(STATE_DIR):
            try:
                os.makedirs(STATE_DIR)
            except OSError as e:
                # Race condition - created at the same time, so it is ok
                if e.errno != errno.EEXIST:
                    raise

    def load_state(self) -> Dict[str, Any]:
        with self._lock:
            if not os.path.exists(STATE_FILE):
                return {}
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                logger.error(f'Error loading state file: {e}. Returning empty state.')
                return {}

    def save_state(self, state: Dict[str, Any]):
        with self._lock:
            # Step 1: Creates a temporal unique file on the same directory.
            tmp_file_path = f'{STATE_FILE}.{uuid.uuid4()}.tmp'

            try:
                # Step 2: writes on the temp file
                with open(tmp_file_path, 'w', encoding='utf-8') as f:
                    json.dump(state, f, indent=2)

                    # Step 3: (Durability): Force writing from cache to disk.
                    f.flush()
                    os.fsync(f.fileno())

                # Step 4 (Atomicity): Rename the temp file
                # It is atomic because it is on the same filesystem
                os.rename(tmp_file_path, STATE_FILE)
                logger.debug(f'State saved successfully to {STATE_FILE}')

            except (IOError, OSError) as e:
                logger.error(f'CRITICAL: Failed to save state durably: {e}')
                if os.path.exists(tmp_file_path):
                    try:
                        os.remove(tmp_file_path)
                    except OSError:
                        pass
                raise
