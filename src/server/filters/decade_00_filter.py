import logging

from src.model.movie import Movie

from .base_filter_logic import BaseFilterLogic

logger = logging.getLogger(__name__)


class Decade00Logic(BaseFilterLogic):
    def should_pass(self, movie: Movie) -> bool:
        try:
            if movie.release_date >= '2000-01-01' and movie.release_date < '2010-01-01':
                return True
            else:
                return False
        except Exception as e:
            movie_id = getattr(movie, 'id', 'UNKNOWN')
            logger.error(f'Error checking movie ID={movie_id} for decade 00: {e}')
            return False

    def map(self, movie: Movie) -> Movie:
        return Movie(movie_id=movie.id, title=movie.title, genres=movie.genres)
