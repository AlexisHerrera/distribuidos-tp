import logging

from src.model.movie import Movie

from .base_filter_logic import BaseFilterLogic

logger = logging.getLogger(__name__)


class ArgentinaLogic(BaseFilterLogic):
    def should_pass(self, movie: Movie) -> bool:
        try:
            if 'Argentina' in movie.production_countries:
                return True
            else:
                # logger.info(f'prod countries: {movie.production_countries}')
                return False
        except Exception as e:
            movie_id = getattr(movie, 'id', 'UNKNOWN')
            logger.error(f'Error checking movie ID={movie_id} for argentina: {e}')
            return False

    def map(self, movie: Movie) -> Movie:
        return Movie(
            movie_id=movie.id,
            title=movie.title,
        )
