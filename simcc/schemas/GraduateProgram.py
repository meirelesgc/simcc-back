import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel


class ResearchLines(BaseModel):
    graduate_program_id: UUID
    name: str
    area: str
    start_year: int | None = None
    end_year: int | None = None


class GraduateProgram(BaseModel):
    graduate_program_id: UUID
    code: Optional[str]
    name: str
    name_en: Optional[str]
    basic_area: Optional[str]
    cooperation_project: Optional[str]
    area: str
    modality: str
    type: Optional[str]
    coordinator: Optional[str]
    email: Optional[str]
    start: Optional[datetime.date]
    phone: Optional[str]
    periodicity: Optional[str]
    rating: Optional[str]
    institution_id: UUID
    state: Optional[str]
    institution: str
    city: Optional[str]
    region: Optional[str]
    url_image: Optional[str]
    acronym: Optional[str]
    description: Optional[str]
    visible: Optional[bool]
    site: Optional[str]
    researchers: List[str] = []
    qtd_permanente: Optional[int]
    qtd_colaborador: Optional[int]
    qtd_estudantes: Optional[int]
    created_at: Optional[datetime.datetime]
    updated_at: Optional[datetime.datetime]
