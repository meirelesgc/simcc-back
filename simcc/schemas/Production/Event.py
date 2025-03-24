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
