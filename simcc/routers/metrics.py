from typing import Annotated, Optional

from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.schemas import DefaultFilters
from simcc.schemas.production.Article import ArticleMetric
from simcc.schemas.production.Guidance import GuidanceMetrics
from simcc.schemas.production.Patent import PatentMetric
from simcc.schemas.production.Software import SoftwareMetric
from simcc.schemas.Researcher import AcademicMetric
from simcc.services import ProductionService

router = APIRouter()

Conn = Annotated[Connection, Depends(get_conn)]
Filters = Annotated[DefaultFilters, Depends()]


@router.get('/magazine_metrics')
async def get_magazine_metrics(
    conn: Conn,
    issn: Optional[str] = None,
    initials: Optional[str] = None,
):
    return await ProductionService.get_magazine_metrics(conn, issn, initials)


@router.get('/researcher_metrics')
async def get_researcher_metrics(conn: Conn, filters: Filters):
    return await ProductionService.get_researcher_metrics(conn, filters)


@router.get('/brand_metrics')
async def get_brand_metrics(
    nature: str = None,
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_brand_metrics(
        conn, nature, default_filters
    )


@router.get('/speaker_metrics')
async def get_speaker_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_speaker_metrics(conn, default_filters)


@router.get('/research_report_metrics')
async def get_research_report_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_research_report_metrics(
        conn, default_filters
    )


@router.get('/events_metrics')
async def get_events_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_events_metrics(conn, default_filters)


@router.get('/papers_magazine_metrics')
async def get_papers_magazine_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_papers_magazine_metrics(
        conn, default_filters
    )


@router.get('/book_metrics')
async def get_book_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_book_metrics(conn, default_filters)


@router.get('/book_chapter_metrics')
async def get_book_chapter_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_book_chapter_metrics(
        conn, default_filters
    )


@router.get(
    '/article_metrics',
    response_model=list[ArticleMetric],
    tags=['Metrics'],
)
async def article_metrics(
    qualis: str = None,
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ProductionService.get_article_metrics(
        conn, qualis, default_filters
    )
    return metrics


@router.get(
    '/patent_metrics',
    response_model=list[PatentMetric],
    tags=['Metrics'],
)
async def patent_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ProductionService.get_patent_metrics(conn, default_filters)
    return metrics


@router.get(
    '/guidance_metrics',
    response_model=list[GuidanceMetrics],
    tags=['Metrics'],
)
async def guidance_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ProductionService.get_guidance_metrics(conn, default_filters)
    return metrics


@router.get(
    '/academic_degree_metrics',
    response_model=list[AcademicMetric],
    tags=['Metrics'],
)
async def academic_degree_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ProductionService.get_academic_degree_metrics(
        conn, default_filters
    )
    return metrics


@router.get(
    '/software_metrics',
    response_model=list[SoftwareMetric],
    tags=['Metrics'],
)
async def software_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ProductionService.get_software_metrics(conn, default_filters)
    return metrics


@router.get(
    '/research_project_metrics',
    tags=['Metrics'],
)
async def get_research_project_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ProductionService.get_research_project_metrics(
        conn, default_filters
    )
    return metrics
