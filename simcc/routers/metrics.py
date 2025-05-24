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


@router.get('/book_metrics', tags=['Metrics'])
def get_book_metrics(
    term: str = None,
    researcher_id: UUID = None,
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
        None,
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
    researcher_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    return ProductionService.get_book_chapter_metrics(
        term, researcher_id, None, year, distinct
    )


@router.get(
    '/researcher_metrics',
    tags=['Metrics'],
)
async def get_researcher_metrics(
    term: str = None,
    year: int = 2020,
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
    ] = None,
    distinct: int = 1,
    institution: str = None,
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_researcher_metrics(
        conn, term, year, type, distinct, institution
    )


@router.get(
    '/article_metrics',
    response_model=list[ArticleMetric],
    tags=['Metrics'],
)
def article_metrics(
    term: str = None,
    program_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_article_metrics(
        term, None, program_id, year, distinct
    )
    return metrics


@router.get(
    '/researcher/{researcher_id}/article_metrics',
    response_model=list[ArticleMetric],
    tags=['Researcher'],
)
def article_metrics_researcher(
    term: str = None,
    researcher_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_article_metrics(
        term, researcher_id, None, year, distinct
    )
    return metrics


@router.get(
    '/graduate_program/{program_id}/article_metrics',
    response_model=list[ArticleMetric],
    tags=['Graduate Program'],
)
def article_metrics_graduate_program(
    term: str = None,
    program_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_article_metrics(
        term, None, program_id, year
    )
    return metrics


@router.get(
    '/patent_metrics',
    response_model=list[PatentMetric],
    tags=['Metrics'],
)
def patent_metrics(
    term: str = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_patent_metrics(
        term, None, None, year, distinct
    )
    return metrics


@router.get(
    '/researcher/{researcher_id}/patent_metrics',
    response_model=list[PatentMetric],
    tags=['Researcher'],
)
def patent_metrics_researcher(
    term: str = None,
    researcher_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_patent_metrics(
        term, researcher_id, None, year, distinct
    )
    return metrics


@router.get(
    '/graduate_program/{program_id}/patent_metrics',
    response_model=list[PatentMetric],
    tags=['Graduate Program'],
)
def patent_metrics_graduate_program(
    term: str = None,
    program_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_patent_metrics(
        term, None, program_id, year, distinct
    )
    return metrics


@router.get(
    '/guidance_metrics',
    response_model=list[GuidanceMetrics],
    tags=['Metrics'],
)
def guidance_metrics(
    term: str = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_guidance_metrics(
        term, None, None, year, distinct
    )
    return metrics


@router.get(
    '/researcher/{researcher_id}/guidance_metrics',
    response_model=list[GuidanceMetrics],
    tags=['Researcher'],
)
def guidance_metrics_researcher(
    term: str = None,
    researcher_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_guidance_metrics(
        term, researcher_id, None, year, distinct
    )
    return metrics


@router.get(
    '/graduate_program/{program_id}/guidance_metrics',
    response_model=list[GuidanceMetrics],
    tags=['Graduate Program'],
)
def guidance_metrics_graduate_program(
    term: str = None,
    program_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_guidance_metrics(
        term, None, program_id, year, distinct
    )
    return metrics


@router.get(
    '/academic_degree_metrics',
    response_model=list[AcademicMetric],
    tags=['Metrics'],
)
def academic_degree_metrics(year: int = 2020):
    metrics = ProductionService.list_academic_degree_metrics(None, None, year)
    return metrics


@router.get(
    '/academic_degree',
    response_model=list[AcademicDegree],
)
def academic_degree():
    metrics = ResearcherService.academic_degree()
    return metrics


@router.get(
    '/researcher/{researcher_id}/academic_degree_metrics',
    response_model=list[AcademicMetric],
    tags=['Researcher'],
)
def academic_degree_metrics_researcher(
    researcher_id: UUID = None, year: int = 2020
):
    metrics = ProductionService.list_academic_degree_metrics(
        researcher_id, None, year
    )
    return metrics


@router.get(
    '/graduate_program/{program_id}/academic_degree_metrics',
    response_model=list[AcademicMetric],
    tags=['Graduate Program'],
)
def academic_degree_metrics_graduate_program(
    program_id: UUID = None, year: int = 2020
):
    metrics = ProductionService.list_academic_degree_metrics(
        None, program_id, year
    )
    return metrics


@router.get(
    '/software_metrics',
    response_model=list[SoftwareMetric],
    tags=['Metrics'],
)
def software_metrics(
    term: str = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_software_metrics(
        term, None, None, year, distinct
    )
    return metrics


@router.get(
    '/researcher/{researcher_id}/software_metrics',
    response_model=list[SoftwareMetric],
    tags=['Researcher'],
)
def software_metrics_researcher(
    term: str = None,
    researcher_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_software_metrics(
        term, researcher_id, None, year, distinct
    )
    return metrics


@router.get(
    '/graduate_program/{program_id}/software_metrics',
    response_model=list[SoftwareMetric],
    tags=['Graduate Program'],
)
def software_metrics_graduate_program(
    term: str = None,
    program_id: UUID = None,
    year: int = 2020,
    distinct: int = 1,
):
    metrics = ProductionService.list_software_metrics(
        term, None, program_id, year, distinct
    )
    return metrics
