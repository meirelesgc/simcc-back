from pydantic import BaseModel


class PapersProduction(BaseModel):
    name: str | list[str]
    title: str
    title_en: str | None
    nature: str
    language: str | None
    means_divulgation: str
    homepage: str | None
    relevance: bool
    scientific_divulgation: bool | None
    authors: str | list | None
    year_: int
    stars: int = 0
