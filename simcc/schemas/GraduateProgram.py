from datetime import datetime
from typing import Optional
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
    name_en: Optional[str]  # Adicionada
    basic_area: Optional[str]  # Adicionada
    cooperation_project: Optional[str]  # Adicionada
    area: str
    modality: str
    type: Optional[str]
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
    coordinator: Optional[str]
    email: Optional[str]
    start: Optional[datetime]
    phone: Optional[datetime]
    periodicity: Optional[str]
    researchers: list = []

    qtd_permanente: Optional[int]
    qtd_colaborador: Optional[int]
    qtd_estudantes: Optional[int]
