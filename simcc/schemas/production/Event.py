from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class EventProduction(BaseModel):
    title: str
    title_en: str | None
    nature: str
    language: str | None
    means_divulgation: str
    homepage: str | None
    relevance: bool = False
    scientific_divulgation: bool | None = False
    authors: str | None
    year_: int
    name: str | list[str]
    id: UUID | list[UUID]
    stars: int = 0

    event_classification: Optional[str]
    event_name: Optional[str]
    event_city: Optional[str]
    event_year: Optional[int]
    proceedings_title: Optional[str]
    volume: Optional[str]
    issue: Optional[str]
    series: Optional[str]
    start_page: Optional[str]
    end_page: Optional[str]
    publisher_name: Optional[str]
    publisher_city: Optional[str]
    event_name_english: Optional[str]
    identifier_number: Optional[str]
    isbn: Optional[str]
