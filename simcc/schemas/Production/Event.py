from pydantic import BaseModel


class EventProduction(BaseModel):
    title: str
    title_en: str | None
    nature: str
    language: str | None
    means_divulgation: str
    homepage: str | None
    relevance: bool = False
    scientific_divulgation: bool = False
    authors: str | None
    year_: int
    name: str | list[str]
