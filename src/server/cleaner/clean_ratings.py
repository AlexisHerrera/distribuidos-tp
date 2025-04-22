import csv
import io
import logging

from src.model.rating import Rating

logger = logging.getLogger(__name__)


def parse_line_to_rating(csv_line: str) -> Rating | None:
    f = io.StringIO(csv_line)
    reader = csv.reader(f, delimiter=',', quotechar='"', skipinitialspace=True)
    try:
        fields = next(reader)
    except StopIteration:
        return None
    except Exception as e:
        logger.warning(f"CSV parsing error for line: '{csv_line[:100]}...': {e}")
        return None

    COL_MOVIE_ID = 1
    COL_RATING = 2
    EXPECTED_COLS = 4

    if len(fields) != EXPECTED_COLS:
        logger.warning(
            f"Incorrect field count ({len(fields)}/{EXPECTED_COLS}) in line: '{csv_line[:100]}...'. Skipping."
        )
        return None

    try:
        movie_id_str = fields[COL_MOVIE_ID].strip()
        if not movie_id_str:
            return None
        movie_id = int(movie_id_str)

        rating = 0
        try:
            rating_str = fields[COL_RATING].strip()
            rating = int(float(rating_str)) if rating_str else 0
        except (ValueError, TypeError):
            pass

        return Rating(movie_id=movie_id, rating=rating)

    except ValueError as e:
        logger.warning(
            f'Value error processing fields for rating in movie {fields[COL_MOVIE_ID]}: {e}. Skipping.'
        )
        return None
    except IndexError as e:
        logger.warning(
            f'Index error processing fields (malformed line?) for rating in movie_id {fields[COL_MOVIE_ID]}: {e}. Skipping.'
        )
        return None
    except Exception as e:
        logger.error(
            f'Unexpected error parsing line for rating in movie_id {fields[COL_MOVIE_ID]}: {e}',
            exc_info=True,
        )
        return None
