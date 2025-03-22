from uuid import UUID

from pydantic import BaseModel


class ReportProduction(BaseModel):
    id: UUID | list[UUID]
    title: str
    name: str | list[str]
    year: int
    project_name: str | None
    financing: str | None
