import logging

from src.model.movie import Movie

from .base_filter_logic import BaseFilterLogic

logger = logging.getLogger(__name__)


class BudgetRevenueLogic(BaseFilterLogic):
    def should_pass(self, movie: Movie) -> bool:
        try:
            if movie.budget != 0 and movie.revenue != 0:
                return True
            else:
                return False
        except Exception as e:
            movie_id = getattr(movie, 'id', 'UNKNOWN')
            logger.error(f'Error checking movie ID={movie_id} for decade 00: {e}')
            return False

    def map(self, movie: Movie) -> Movie:
        return Movie(
            movie_id=movie.id,
            title=movie.title,
            budget=movie.budget,
            revenue=movie.revenue,
            overview=movie.overview,
        )
