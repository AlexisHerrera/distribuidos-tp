from typing import Any


class MovieRatingCount:
    def __init__(self, movie_id: int, title: str, partial_sum: float, count: int):
        self.movie_id = movie_id
        self.title = title
        self.partial_sum = partial_sum
        self.count = count

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'MovieRatingCount':
        return cls(**data)
