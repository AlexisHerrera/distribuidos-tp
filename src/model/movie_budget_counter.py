from typing import Any


class MovieBudgetCounter:
    def __init__(self, country: str, total_budget: int):
        self.country = country
        self.total_budget = total_budget

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'MovieBudgetCounter':
        return cls(**data)
