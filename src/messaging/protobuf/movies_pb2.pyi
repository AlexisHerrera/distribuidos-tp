from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Movie(_message.Message):
    __slots__ = (
        'id',
        'title',
        'genres',
        'release_date',
        'production_countries',
        'budget',
        'revenue',
        'overview',
    )
    ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    GENRES_FIELD_NUMBER: _ClassVar[int]
    RELEASE_DATE_FIELD_NUMBER: _ClassVar[int]
    PRODUCTION_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    BUDGET_FIELD_NUMBER: _ClassVar[int]
    REVENUE_FIELD_NUMBER: _ClassVar[int]
    OVERVIEW_FIELD_NUMBER: _ClassVar[int]
    id: int
    title: str
    genres: _containers.RepeatedScalarFieldContainer[str]
    release_date: str
    production_countries: _containers.RepeatedScalarFieldContainer[str]
    budget: int
    revenue: int
    overview: str
    def __init__(
        self,
        id: _Optional[int] = ...,
        title: _Optional[str] = ...,
        genres: _Optional[_Iterable[str]] = ...,
        release_date: _Optional[str] = ...,
        production_countries: _Optional[_Iterable[str]] = ...,
        budget: _Optional[int] = ...,
        revenue: _Optional[int] = ...,
        overview: _Optional[str] = ...,
    ) -> None: ...

class Movies(_message.Message):
    __slots__ = ('list',)
    LIST_FIELD_NUMBER: _ClassVar[int]
    list: _containers.RepeatedCompositeFieldContainer[Movie]
    def __init__(
        self, list: _Optional[_Iterable[_Union[Movie, _Mapping]]] = ...
    ) -> None: ...
