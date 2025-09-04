from uuid import UUID

from pydantic import BaseModel


class SoftwareMetric(BaseModel):
    year: int
    among: int


class SoftwareProduction(BaseModel):
    title: str
    year: int
    has_image: bool | None = False
    relevance: bool | None = False
    name: str | list[str]
    id: UUID | list[UUID]
