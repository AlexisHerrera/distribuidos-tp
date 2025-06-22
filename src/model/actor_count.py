from typing import Any


class ActorCount:
    def __init__(self, actor_name: str, count: int):
        self.actor_name = actor_name
        self.count = count

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'ActorCount':
        return cls(**data)
