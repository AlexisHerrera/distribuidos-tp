import logging

from src.model.movie import Movie

from .base_filter_logic import BaseFilterLogic

logger = logging.getLogger(__name__)


class ArgentinaAndSpainLogic(BaseFilterLogic):
    def should_pass(self, movie: Movie) -> bool:
        try:
            if (
                'Argentina' in movie.production_countries
                and 'Spain' in movie.production_countries
            ):
                return True
            else:
                return False
        except Exception as e:
            movie_id = getattr(movie, 'id', 'UNKNOWN')
            logger.error(
                f'Error checking movie ID={movie_id} for argentina and spain: {e}'
            )
            return False

    def map(self, movie: Movie) -> Movie:
        return Movie(
            movie_id=movie.id,
            title=movie.title,
            genres=movie.genres,
            release_date=movie.release_date,
        )
