import struct
import logging
from enum import Enum
from typing import List

from src.model.cast import Cast
from src.model.movie import Movie
from src.model.rating import Rating
from src.server.cleaner.clean_credits import parse_line_to_credits
from src.server.cleaner.clean_movies import parse_line_to_movie
from src.server.cleaner.clean_ratings import parse_line_to_rating

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


def batch_to_list_objects(batch: Batch) -> list:
    if batch.type == BatchType.MOVIES:
        return _batch_data_to_movies(batch.data)
    elif batch.type == BatchType.CREDITS:
        return _batch_data_to_credits(batch.data)
    elif batch.type == BatchType.RATINGS:
        return _batch_data_to_ratings(batch.data)
    else:
        logger.warning(f'No parser implemented for BatchType: {batch.type.name}')
        return []


def _batch_data_to_movies(data_lines: list[str]) -> list[Movie]:
    parsed_movies = []
    for line in data_lines:
        if not line.strip():
            continue
        movie = parse_line_to_movie(line)
        if movie:
            parsed_movies.append(movie)

    return parsed_movies


def _batch_data_to_credits(data_lines: list[str]) -> list[Cast]:
    parsed_credits = []
    for line in data_lines:
        if not line.strip():
            continue
        credits = parse_line_to_credits(line)
        if credits:
            parsed_credits.append(credits)

    return parsed_credits


def _batch_data_to_ratings(data_lines: list[str]) -> list[Rating]:
    parsed_ratings = []
    for line in data_lines:
        if not line.strip():
            continue
        rating = parse_line_to_rating(line)
        if rating:
            parsed_ratings.append(rating)

    return parsed_ratings
