from uuid import UUID

from pydantic import BaseModel


class BrandProduction(BaseModel):
    title: str
    year: int
    has_image: bool | None
    relevance: bool | None
    lattes_id: str | list[str]
    name: str | list[str]
    id: UUID | list[UUID]
    stars: int = 0
