from uuid import UUID

from pydantic import BaseModel


class GuidanceMetrics(BaseModel):
    year: int
    m_completed: int
    m_in_progress: int
    ic_completed: int
    ic_in_progress: int
    d_completed: int
    d_in_progress: int
    g_completed: int
    g_in_progress: int
    e_completed: int
    e_in_progress: int
    sd_completed: int
    sd_in_progress: int


class GuidanceProduction(BaseModel):
    id: UUID | None
    title: str | None
    nature: str
    oriented: str
    type: str | None
    status: str
    year: int
    name: str | list[str]
