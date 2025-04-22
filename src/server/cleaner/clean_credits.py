import ast
import csv
import io
import logging

from src.model.cast import Cast

logger = logging.getLogger(__name__)


def parse_line_to_credits(csv_line: str) -> Cast | None:
    f = io.StringIO(csv_line)
    reader = csv.reader(f, delimiter=',', quotechar='"', skipinitialspace=True)
    try:
        fields = next(reader)
    except StopIteration:
        return None
    except Exception as e:
        logger.warning(f"CSV parsing error for line: '{csv_line[:100]}...': {e}")
        return None

    COL_MOVIE_ID = 2
    COL_CAST = 0
    EXPECTED_COLS = 3

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

        cast_list = []
        try:
            cast_str = fields[COL_CAST].strip()
            if cast_str:
                cast_data = ast.literal_eval(cast_str)
                if isinstance(cast_data, list):
                    cast_list = [
                        c['name']
                        for c in cast_data
                        if isinstance(c, dict) and 'name' in c and c['name']
                    ]
        except (ValueError, SyntaxError, TypeError):
            pass

        return Cast(
            movie_id=movie_id,
            cast=cast_list,
        )

    except ValueError as e:
        logger.warning(
            f'Value error processing fields for cast in movie {fields[COL_MOVIE_ID]}: {e}. Skipping.'
        )
        return None
    except IndexError as e:
        logger.warning(
            f'Index error processing fields (malformed line?) for cast in movie_id {fields[COL_MOVIE_ID]}: {e}. Skipping.'
        )
        return None
    except Exception as e:
        logger.error(
            f'Unexpected error parsing line for cast in movie_id {fields[COL_MOVIE_ID]}: {e}',
            exc_info=True,
        )
        return None
