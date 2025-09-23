from uuid import UUID

from pydantic import BaseModel


class ProfessionalExperience(BaseModel):
    id: UUID
    researcher_id: UUID
    enterprise: str | None
    start_year: int | None
    end_year: int | None
    employment_type: str | None
    other_employment_type: str | None
    functional_classification: str | None
    other_functional_classification: str | None
    workload_hours_weekly: str | None
    exclusive_dedication: bool | None
    additional_info: str | None
    stars: int = 0
