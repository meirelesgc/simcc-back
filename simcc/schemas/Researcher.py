from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel


class ResearcherMetric(BaseModel): ...


class ResearcherArticleProduction(BaseModel):
    name: str
    A1: int
    A2: int
    A3: int
    A4: int
    B1: int
    B2: int
    B3: int
    B4: int
    C: int
    SQ: int
    citations: int
    year: int


class ResearcherUFMG(BaseModel):
    researcher_id: UUID
    full_name: Optional[str] = None
    gender: Optional[str] = None
    status_code: Optional[str] = None
    work_regime: Optional[str] = None
    job_class: Optional[int] = None
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


class Researcher(BaseModel):
    # Researcher Data
    id: UUID
    name: str
    lattes_id: str
    lattes_10_id: str
    university: str
    abstract: str
    area: str
    city: str
    image_university: str
    orcid: str | str
    graduation: str
    lattes_update: datetime
    classification: str
    status: bool
    institution_id: UUID

    # Metrics
    among: int | str
    articles: int | str
    book_chapters: int | str
    book: int | str
    patent: int | str
    software: int | str
    brand: int | str

    # OpenAlex Data
    h_index: int | str
    relevance_score: int | str
    works_count: int | str
    cited_by_count: int | str
    i10_index: int | str
    scopus: str | str
    openalex: str | str

    # Miscellaneous
    research_groups: list | str
    subsidy: list | str
    departments: list | str
    graduate_programs: list | str

    ufmg: ResearcherUFMG | Any
    user: dict | str

    class Config:
        json_encoders = {datetime: lambda v: v.strftime('%d/%m/%Y')}


class AcademicDegree(BaseModel):
    graduation: str
    among: int


class AcademicMetric(BaseModel):
    year: int
    doctorate_end: int
    doctorate_start: int
    masters_degree_end: int
    masters_degree_start: int
    undergraduate_end: int
    undergraduate_start: int
    specialization_start: int
    specialization_end: int
    professional_masters_degree_start: int
    professional_masters_degree_end: int


class CoAuthorship(BaseModel):
    name: str
    among: int
    type: str
