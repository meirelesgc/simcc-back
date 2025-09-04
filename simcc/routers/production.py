from typing import Annotated

from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_admin_conn, get_conn
from simcc.schemas import DefaultFilters, QualisOptions
from simcc.schemas.production.Article import ArticleProduction
from simcc.schemas.production.Book import BookProduction
from simcc.schemas.production.BookChapter import BookChapterProduction
from simcc.schemas.production.Brand import BrandProduction
from simcc.schemas.production.Event import EventProduction
from simcc.schemas.production.Guidance import GuidanceProduction
from simcc.schemas.production.Papers import PapersProduction
from simcc.schemas.production.Patent import PatentProduction
from simcc.schemas.production.ProfessionalExperience import (
    ProfessionalExperience,
)
from simcc.schemas.production.Report import ReportProduction
from simcc.schemas.production.ResearchProject import ResearchProjectProduction
from simcc.schemas.production.Software import SoftwareProduction
from simcc.services import ProductionService

router = APIRouter(tags=['Production'])

Conn = Annotated[Connection, Depends(get_conn)]
AdminConn = Annotated[Connection, Depends(get_admin_conn)]
Filters = Annotated[DefaultFilters, Depends()]


@router.get(
    '/production/professional-experience',
    response_model=list[ProfessionalExperience],
)
@router.get('/professional_experience', include_in_schema=False)
async def list_professional_experience(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_professional_experience(
        conn, conn_admin, filters
    )


@router.get('/production/patent', response_model=list[PatentProduction])
@router.get('/patent_production_researcher', include_in_schema=False)
async def list_patent_production(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_patent(conn, conn_admin, filters)


@router.get('/production/events/participation')
@router.get('/pevent_researcher', include_in_schema=False)
async def get_pevent_researcher(
    conn: Conn, conn_admin: AdminConn, filters: Filters, nature: str = None
):
    return await ProductionService.get_pevent_researcher(
        conn, conn_admin, filters, nature
    )


@router.get('/production/book', response_model=list[BookProduction])
@router.get('/book_production_researcher', include_in_schema=False)
async def list_book_production(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_book(conn, conn_admin, filters)


@router.get('/production/brand', response_model=list[BrandProduction])
@router.get('/brand_production_researcher', include_in_schema=False)
async def list_brand_production(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_brand(conn, conn_admin, filters)


@router.get('/production/report', response_model=list[ReportProduction])
@router.get('/researcher_report', include_in_schema=False)
async def list_researcher_report(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_researcher_report(
        conn, conn_admin, filters
    )


@router.get(
    '/production/book-chapter', response_model=list[BookChapterProduction]
)
@router.get('/book_chapter_production_researcher', include_in_schema=False)
async def list_book_chapter_production(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_book_chapter(conn, conn_admin, filters)


@router.get('/outstanding_articles', include_in_schema=False)
async def list_outstanding_articles(conn: Connection = Depends(get_conn)):
    articles = await ProductionService.list_bibliographic_production(
        conn,
        DefaultFilters(type='ARTICLE', distinct=1, year=20),
        None,
        None,
        None,
    )
    return articles


@router.get('/production/article', response_model=list[ArticleProduction])
@router.get('/bibliographic_production_article', include_in_schema=False)
@router.get('/bibliographic_production_researcher', include_in_schema=False)
async def list_bibliographic_production(
    conn: Conn,
    conn_admin: AdminConn,
    filters: Filters,
    terms: str | None = None,
    qualis: QualisOptions | str = None,
):
    if terms:
        filters.term = terms
    return await ProductionService.list_bibliographic_production(
        conn, conn_admin, filters, qualis
    )


@router.get('/production/software', response_model=list[SoftwareProduction])
@router.get('/software_production_researcher', include_in_schema=False)
async def list_software_production(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_software(conn, conn_admin, filters)


@router.get('/production/guidance', response_model=list[GuidanceProduction])
@router.get('/guidance_researcher', include_in_schema=False)
async def list_guidance_production(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_guidance_production(
        conn, conn_admin, filters
    )


@router.get(
    '/production/research-project',
    response_model=list[ResearchProjectProduction],
)
@router.get('/researcher_research_project', include_in_schema=False)
async def list_research_project(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_research_projects(
        conn, conn_admin, filters
    )


@router.get('/production/paper', response_model=list[PapersProduction])
@router.get('/researcher_production/papers_magazine', include_in_schema=False)
async def list_papers_magazine(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_papers_magazine(
        conn, conn_admin, filters
    )


@router.get('/production/event/article', response_model=list[EventProduction])
@router.get('/researcher_production/events', include_in_schema=False)
async def list_researcher_production_events(
    conn: Conn, conn_admin: AdminConn, filters: Filters
):
    return await ProductionService.list_researcher_production_events(
        conn, conn_admin, filters
    )
