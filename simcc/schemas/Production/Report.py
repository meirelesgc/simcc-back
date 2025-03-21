from uuid import UUID

from pydantic import BaseModel


class ReportProduction(BaseModel):
    id: UUID | None
    title: str
    name: str | list[str]
    year: int
    project_name: str | None
    financing: str | None
