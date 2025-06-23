import logging
import os
import time
import uuid
from typing import Tuple

logger = logging.getLogger(__name__)


class WALManager:
    def __init__(self, wal_directory: str):
        self.wal_dir = wal_directory
        try:
            os.makedirs(self.wal_dir, exist_ok=True)
        except OSError as e:
            logger.critical(f'Could not create WAL directory at {self.wal_dir}: {e}')
            raise

    def write_entry(self, user_id: uuid.UUID, batch_type_name: str, data: bytes) -> str:
        timestamp = time.time()
        filename = f'{user_id}_{timestamp}_{batch_type_name}.wal'
        filepath = os.path.join(self.wal_dir, filename)

        try:
            with open(filepath, 'wb') as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            logger.debug(f'Durably wrote batch to WAL file: {filename}')
            return filepath
        except (IOError, OSError) as e:
            logger.critical(
                f'CRITICAL: Could not write to WAL file {filepath}. Data might be lost. Error: {e}'
            )
            raise

    def remove_entry(self, wal_filepath: str):
        try:
            os.remove(wal_filepath)
            logger.debug(
                f'Removed processed WAL file: {os.path.basename(wal_filepath)}'
            )
        except FileNotFoundError:
            logger.warning(
                f'Attempted to remove WAL file that does not exist: {wal_filepath}'
            )
        except (IOError, OSError) as e:
            logger.error(f'Error removing WAL file {wal_filepath}: {e}')

    def recover(self) -> list[Tuple[str, bytes]]:
        pending_files = []
        logger.info(f'Scanning for pending batches in WAL directory: {self.wal_dir}')
        for filename in os.listdir(self.wal_dir):
            if filename.endswith('.wal'):
                try:
                    timestamp = float(filename.split('_')[1])
                    filepath = os.path.join(self.wal_dir, filename)
                    pending_files.append((timestamp, filepath))
                except (IndexError, ValueError):
                    logger.warning(
                        f'Could not parse timestamp from WAL file: {filename}. Skipping.'
                    )

        pending_files.sort()

        recovered_batches = []
        for _, filepath in pending_files:
            try:
                with open(filepath, 'rb') as f:
                    data = f.read()
                recovered_batches.append((filepath, data))
            except (IOError, OSError) as e:
                logger.error(f'Failed to read WAL file {filepath} during recovery: {e}')

        if recovered_batches:
            logger.warning(f'Found {len(recovered_batches)} batches to recover.')
        else:
            logger.info('No pending batches found to recover.')

        return recovered_batches
