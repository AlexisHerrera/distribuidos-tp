from abc import ABC, abstractmethod
from src.model.movie import Movie


class BaseFilterLogic(ABC):
    @abstractmethod
    def should_pass(self, movie: Movie) -> bool:
        pass

    @abstractmethod
    def setup(self):
        pass
