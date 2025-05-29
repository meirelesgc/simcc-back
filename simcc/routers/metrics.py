from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.schemas.Production.Article import ArticleMetric
from simcc.schemas.Production.Guidance import GuidanceMetrics
from simcc.schemas.Production.Patent import PatentMetric
from simcc.schemas.Production.Software import SoftwareMetric
from simcc.schemas.Researcher import AcademicDegree, AcademicMetric
from simcc.services import ProductionService, ResearcherService

router = APIRouter()


@router.get('/speaker_metrics', tags=['Metrics'])
def get_speaker_metrics(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    return ProductionService.get_speaker_metrics(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )


@router.get('/book_metrics', tags=['Metrics'])
def get_book_metrics(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    return ProductionService.get_book_metrics(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )


@router.get('/book_chapter_metrics', tags=['Metrics'])
def get_book_chapter_metrics(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    return ProductionService.get_book_chapter_metrics(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )


@router.get(
    '/researcher_metrics',
    tags=['Metrics'],
)
async def get_researcher_metrics(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 0,
    type: Literal[
        'BOOK',
        'BOOK_CHAPTER',
        'ARTICLE',
        'WORK_IN_EVENT',
        'TEXT_IN_NEWSPAPER_MAGAZINE',
        'ABSTRACT',
        'PATENT',
        'AREA',
        'EVENT',
        'NAME',
    ] = None,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_researcher_metrics(
        conn,
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        type,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )


@router.get(
    '/article_metrics',
    response_model=list[ArticleMetric],
    tags=['Metrics'],
)
def article_metrics(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    metrics = ProductionService.list_article_metrics(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )
    return metrics


@router.get(
    '/patent_metrics',
    response_model=list[PatentMetric],
    tags=['Metrics'],
)
def patent_metrics(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    metrics = ProductionService.list_patent_metrics(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )
    return metrics


@router.get(
    '/guidance_metrics',
    response_model=list[GuidanceMetrics],
    tags=['Metrics'],
)
def guidance_metrics(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    metrics = ProductionService.list_guidance_metrics(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )
    return metrics


@router.get(
    '/academic_degree_metrics',
    response_model=list[AcademicMetric],
    tags=['Metrics'],
)
def academic_degree_metrics(
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    metrics = ProductionService.list_academic_degree_metrics(
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )
    return metrics


@router.get(
    '/academic_degree',
    response_model=list[AcademicDegree],
)
def academic_degree():
    metrics = ResearcherService.academic_degree()
    return metrics


@router.get(
    '/software_metrics',
    response_model=list[SoftwareMetric],
    tags=['Metrics'],
)
def software_metrics(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    metrics = ProductionService.list_software_metrics(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
    )
    return metrics
