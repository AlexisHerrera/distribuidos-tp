import logging

from src.model.movie import Movie

from .base_filter_logic import BaseFilterLogic

logger = logging.getLogger(__name__)


class SingleCountryLogic(BaseFilterLogic):
    def should_pass(self, movie: Movie) -> bool:
        try:
            production_countries = list(movie.production_countries)
            if len(production_countries) == 1:
                return True
            else:
                return False
        except Exception as e:
            movie_id = getattr(movie, 'id', 'UNKNOWN')
            logger.error(f'Error checking movie ID={movie_id} for solo country: {e}')
            return False

    def map(self, movie: Movie) -> Movie:
        return Movie(
            movie_id=movie.id,
            title=movie.title,
            production_countries=movie.production_countries,
            budget=movie.budget,
        )
