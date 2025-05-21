from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class ResearcherData(BaseModel):
    nome: str = Field(..., max_length=255)
    cpf: str = Field(..., max_length=14)
    classe: int
    nivel: int
    inicio: datetime
    fim: datetime | None
    tempo_nivel: int | None
    tempo_acumulado: int
    arquivo: str = Field(..., max_length=255)


class RtMetrics(BaseModel):
    researchers: list
    technicians: list


class Technician(BaseModel):
    technician_id: UUID
    full_name: Optional[str] = None
    gender: Optional[str] = None
    status_code: Optional[str] = None
    work_regime: Optional[str] = None
    job_class: Optional[str] = None
    job_title: Optional[str] = None
    job_rank: Optional[str] = None
    job_reference_code: Optional[str] = None
    academic_degree: Optional[str] = None
    organization_entry_date: Optional[date] = None
    last_promotion_date: Optional[date] = None
    employment_status_description: Optional[str] = None
    department_name: Optional[str] = None
    career_category: Optional[str] = None
    academic_unit: Optional[str] = None
    unit_code: Optional[str] = None
    function_code: Optional[str] = None
    position_code: Optional[str] = None
    leadership_start_date: Optional[date] = None
    leadership_end_date: Optional[date] = None
    current_function_name: Optional[str] = None
    function_location: Optional[str] = None
    registration_number: Optional[str] = None
    ufmg_registration_number: Optional[str] = None
    semester_reference: Optional[str] = None
