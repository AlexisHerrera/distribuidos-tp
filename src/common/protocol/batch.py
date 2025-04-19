import struct
import logging
from enum import Enum
from typing import List

logger = logging.getLogger(__name__)


class BatchType(Enum):
    UNKNOWN = 0
    MOVIES = 1
    RATINGS = 2
    CREDITS = 3
    EOF = 100

    @classmethod
    def _missing_(cls, value):
        logger.warning(f'Received unknown BatchType value: {value}')
        return BatchType.UNKNOWN


class Batch:
    HEADER_FORMAT = '>BI'  # B = 1 byte unsigned char, I = 4 bytes unsigned int
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, batch_type: BatchType, data: List[str] | None):
        if not isinstance(batch_type, BatchType):
            raise TypeError('batch_type must be an instance of BatchType Enum')
        if data is not None and not isinstance(data, list):
            raise TypeError('data must be a list of strings or None')

        self.type = batch_type
        self.data: List[str] = data if data is not None else []

    def to_bytes(self) -> bytes:
        try:
            payload_str = '\n'.join(self.data)
            payload_bytes = payload_str.encode('utf-8')
            payload_len = len(payload_bytes)

            header = struct.pack(Batch.HEADER_FORMAT, self.type.value, payload_len)

            return header + payload_bytes
        except Exception as e:
            logger.error(f'Error during Batch serialization: {e}', exc_info=True)
            return b''

    @classmethod
    def from_bytes(cls, raw_bytes: bytes) -> 'Batch | None':
        if len(raw_bytes) < cls.HEADER_SIZE:
            logger.error(
                f'Received insufficient bytes ({len(raw_bytes)}) to decode Batch header ({cls.HEADER_SIZE})'
            )
            return None

        try:
            batch_type_value, payload_len = struct.unpack(
                cls.HEADER_FORMAT, raw_bytes[: cls.HEADER_SIZE]
            )
            batch_type = BatchType(batch_type_value)

            payload_bytes = raw_bytes[cls.HEADER_SIZE : cls.HEADER_SIZE + payload_len]

            if len(payload_bytes) != payload_len:
                logger.error(
                    f'Data corruption? Expected payload len {payload_len}, got {len(payload_bytes)}'
                )
                return None

            data_list: List[str] = []
            if payload_len > 0:
                payload_str = payload_bytes.decode('utf-8')
                data_list = payload_str.split('\n')

            return cls(batch_type, data_list)

        except struct.error as e:
            logger.error(
                f'Struct unpack error during Batch deserialization: {e}', exc_info=True
            )
            return None
        except UnicodeDecodeError as e:
            logger.error(
                f'UTF-8 decode error during Batch deserialization: {e}', exc_info=True
            )
            return None
        except Exception as e:
            logger.error(
                f'Unexpected error during Batch deserialization: {e}', exc_info=True
            )
            return None

    def __repr__(self):
        return f'Batch(type={self.type.name}, data_lines={len(self.data)})'
