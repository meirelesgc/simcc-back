from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.schemas import (
    DefaultFilters,  # Presumindo que DefaultFilters já tem todos os campos opcionais
)
from simcc.schemas.Production.Article import ArticleMetric
from simcc.schemas.Production.Guidance import GuidanceMetrics
from simcc.schemas.Production.Patent import PatentMetric
from simcc.schemas.Production.Software import SoftwareMetric
from simcc.schemas.Researcher import AcademicDegree, AcademicMetric
from simcc.services import ProductionService, ResearcherService

router = APIRouter()


@router.get(
    '/researcher_metrics',
    tags=['Metrics'],
)
async def get_researcher_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_researcher_metrics(conn, default_filters)


@router.get('/brand_metrics', tags=['Metrics'])
async def get_brand_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_brand_metrics(conn, default_filters)


@router.get('/speaker_metrics', tags=['Metrics'])
async def get_speaker_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_speaker_metrics(conn, default_filters)


@router.get('/research_report_metrics', tags=['Metrics'])
async def get_research_report_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_research_report_metrics(
        conn, default_filters
    )


@router.get('/papers_magazine_metrics', tags=['Metrics'])
async def get_papers_magazine_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_papers_magazine_metrics(
        conn, default_filters
    )


@router.get('/book_metrics', tags=['Metrics'])
async def get_book_metrics(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.get_book_metrics(conn, default_filters)


@router.get('/book_chapter_metrics', tags=['Metrics'])
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
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ProductionService.get_article_metrics(conn, default_filters)
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
    '/academic_degree',
    response_model=list[AcademicDegree],
)
async def academic_degree(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ResearcherService.get_academic_degree(conn, default_filters)
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
