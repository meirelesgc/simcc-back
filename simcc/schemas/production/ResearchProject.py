from uuid import UUID

from pydantic import BaseModel


class ResearchProjectProduction(BaseModel):
    id: UUID | list[UUID]
    researcher_id: UUID | list[UUID]
    name: str | list[str]
    start_year: int | None
    end_year: int | None
    agency_code: str | None
    agency_name: str | None
    project_name: str
    status: str
    nature: str
    number_undergraduates: int | None
    number_specialists: int | None
    number_academic_masters: int | None
    number_phd: int | None
    description: str | None
    production: list | None
    foment: list | None
    components: list | None
