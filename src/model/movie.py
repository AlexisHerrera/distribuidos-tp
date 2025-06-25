from typing import Any


class Movie:
    def __init__(
        self,
        movie_id: int,
        title: str,
        genres: list[str] = None,
        release_date: str = '1900-01-01',
        production_countries: list[str] = None,
        budget: int = 0,
        revenue: int = 0,
        overview: str = '',
    ):
        self.id = movie_id
        self.title = title
        self.genres = genres
        self.release_date = release_date
        self.production_countries = production_countries
        self.budget = budget
        self.revenue = revenue
        self.overview = overview

    def to_dict(self) -> dict[str, Any]:
        return {
            'movie_id': self.id,
            'title': self.title,
            'genres': list(self.genres) if self.genres else [],
            'release_date': self.release_date,
            'production_countries': list(self.production_countries)
            if self.production_countries
            else [],
            'budget': self.budget,
            'revenue': self.revenue,
            'overview': self.overview,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'Movie':
        return cls(**data)
