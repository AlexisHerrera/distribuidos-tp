from enum import IntEnum

class MovieCSVField(IntEnum):
    BUDGET = 2
    GENRES = 3
    MOVIE_ID = 5
    OVERVIEW = 10
    PRODUCTION_COUNTRIES = 13
    RELEASE_DATE = 14
    REVENUE = 15
    TITLE = 20

MIN_EXPECTED_FIELDS = 24
