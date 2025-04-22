import logging

from src.model.movie import Movie

from .base_filter_logic import BaseFilterLogic

logger = logging.getLogger(__name__)


class Post2000Logic(BaseFilterLogic):
    def should_pass(self, movie: Movie) -> bool:
        try:
            if movie.release_date >= '2000-01-01':
                return True
            else:
                return False
        except Exception as e:
            movie_id = getattr(movie, 'id', 'UNKNOWN')
            logger.error(f'Error checking movie ID={movie_id} for post 2000: {e}')
            return False

    def map(self, movie: Movie) -> Movie:
        return Movie(
            movie_id=movie.id,
            title=movie.title,
            release_date=movie.release_date,
            production_countries=movie.production_countries,
        )
