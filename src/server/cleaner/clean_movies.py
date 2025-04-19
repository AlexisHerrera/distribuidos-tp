import ast
import csv
import io
import logging

from src.model.movie import Movie

logger = logging.getLogger(__name__)


def parse_line_to_movie(csv_line: str) -> Movie | None:
    f = io.StringIO(csv_line)
    reader = csv.reader(f, delimiter=',', quotechar='"', skipinitialspace=True)
    try:
        fields = next(reader)
    except StopIteration:
        return None
    except Exception as e:
        logger.warning(f"CSV parsing error for line: '{csv_line[:100]}...': {e}")
        return None

    COL_ID = 5
    COL_TITLE = 20
    COL_GENRES = 3
    COL_RELEASE_DATE = 14
    COL_PROD_COUNTRIES = 13
    COL_BUDGET = 2
    COL_REVENUE = 15
    COL_OVERVIEW = 9
    COL_ORIGINAL_TITLE = 8
    EXPECTED_COLS = 24

    if len(fields) != EXPECTED_COLS:
        logger.warning(
            f"Incorrect field count ({len(fields)}/{EXPECTED_COLS}) in line: '{csv_line[:100]}...'. Skipping."
        )
        return None

    try:
        movie_id_str = fields[COL_ID].strip()
        if not movie_id_str:
            return None
        movie_id = int(movie_id_str)

        title = fields[COL_TITLE].strip()
        if not title:
            title = fields[COL_ORIGINAL_TITLE].strip()
        if not title:
            return None

        overview = fields[COL_OVERVIEW].strip()
        release_date = (
            fields[COL_RELEASE_DATE].strip()
            if fields[COL_RELEASE_DATE].strip()
            else '1900-01-01'
        )

        budget = 0
        try:
            budget_str = fields[COL_BUDGET].strip()
            budget = int(float(budget_str)) if budget_str else 0
        except (ValueError, TypeError):
            pass

        revenue = 0
        try:
            revenue_str = fields[COL_REVENUE].strip()
            revenue = int(float(revenue_str)) if revenue_str else 0
        except (ValueError, TypeError):
            pass

        genres_list = []
        try:
            genres_str = fields[COL_GENRES].strip()
            if genres_str:
                genres_data = ast.literal_eval(genres_str)
                if isinstance(genres_data, list):
                    genres_list = [
                        g['name']
                        for g in genres_data
                        if isinstance(g, dict) and 'name' in g and g['name']
                    ]
        except (ValueError, SyntaxError, TypeError):
            pass

        countries_list = []
        try:
            countries_str = fields[COL_PROD_COUNTRIES].strip()
            if countries_str:
                countries_data = ast.literal_eval(countries_str)
                if isinstance(countries_data, list):
                    countries_list = [
                        c['name']
                        for c in countries_data
                        if isinstance(c, dict) and 'name' in c and c['name']
                    ]
        except (ValueError, SyntaxError, TypeError):
            pass

        return Movie(
            movie_id=movie_id,
            title=title,
            genres=genres_list,
            release_date=release_date,
            production_countries=countries_list,
            budget=budget,
            revenue=revenue,
            overview=overview,
        )

    except ValueError as e:
        logger.warning(
            f'Value error processing fields for movie_id {fields[COL_ID]}: {e}. Skipping.'
        )
        return None
    except IndexError as e:
        logger.warning(
            f'Index error processing fields (malformed line?) for movie_id {fields[COL_ID]}: {e}. Skipping.'
        )
        return None
    except Exception as e:
        logger.error(
            f'Unexpected error parsing line for movie_id {fields[COL_ID]}: {e}',
            exc_info=True,
        )
        return None
