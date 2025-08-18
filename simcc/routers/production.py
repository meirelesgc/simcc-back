from typing import Annotated

from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.schemas import ArticleOptions, DefaultFilters, QualisOptions
from simcc.schemas.Production.Article import ArticleProduction
from simcc.schemas.Production.Book import BookProduction
from simcc.schemas.Production.BookChapter import BookChapterProduction
from simcc.schemas.Production.Brand import BrandProduction
from simcc.schemas.Production.Event import EventProduction
from simcc.schemas.Production.Guidance import GuidanceProduction
from simcc.schemas.Production.Papers import PapersProduction
from simcc.schemas.Production.Patent import PatentProduction
from simcc.schemas.Production.ProfessionalExperience import (
    ProfessionalExperience,
)
from simcc.schemas.Production.Report import ReportProduction
from simcc.schemas.Production.ResearchProject import ResearchProjectProduction
from simcc.schemas.Production.Software import SoftwareProduction
from simcc.services import ProductionService

router = APIRouter()

Conn = Annotated[Connection, Depends(get_conn)]
Filters = Annotated[DefaultFilters, Depends()]


@router.get(
    '/professional_experience',
    response_model=list[ProfessionalExperience],
)
async def get_professional_experience(conn: Conn, filters: Filters):
    return await ProductionService.professional_experience(conn, filters)


@router.get(
    '/patent_production_researcher', response_model=list[PatentProduction]
)
async def list_patent_production(conn: Conn, filters: Filters):
    return await ProductionService.list_patent(conn, filters)


@router.get('/pevent_researcher')
async def get_pevent_researcher(
    conn: Conn, filters: Filters, nature: str = None
):
    return await ProductionService.get_pevent_researcher(conn, filters, nature)


@router.get(
    '/book_production_researcher',
    response_model=list[BookProduction],
)
async def list_book_production(conn: Conn, filters: Filters):
    return await ProductionService.list_book(conn, filters)


@router.get(
    '/brand_production_researcher',
    response_model=list[BrandProduction],
)
async def list_brand_production(conn, filters):
    return await ProductionService.list_brand(conn, filters)


@router.get(
    '/researcher_report',
    response_model=list[ReportProduction],
)
async def list_researcher_report(
    default_filters: DefaultFilters = Depends(),
    page: int | None = None,
    lenght: int | None = None,
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.list_researcher_report(
        conn, default_filters, page, lenght
    )


@router.get(
    '/book_chapter_production_researcher',
    response_model=list[BookChapterProduction],
)
async def list_book_chapter_production(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    result = await ProductionService.list_book_chapter(conn, default_filters)
    return result


@router.get(
    '/outstanding_articles',
    response_model=list[ArticleProduction],
)
async def list_outstanding_articles(
    conn: Connection = Depends(get_conn),
):
    articles = await ProductionService.list_bibliographic_production(
        conn,
        DefaultFilters(type='ARTICLE', distinct=1, year=20),
        None,
        None,
        None,
    )
    return articles


@router.get(
    '/bibliographic_production_researcher',
    response_model=list[ArticleProduction],
)
async def list_bibliographic_production(
    default_filters: DefaultFilters = Depends(),
    terms: str | None = None,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions | str = None,
    conn: Connection = Depends(get_conn),
):
    default_filters.type = type
    if terms:
        default_filters.term = terms
    articles = await ProductionService.list_bibliographic_production(
        conn, default_filters, qualis
    )
    return articles


@router.get(
    '/software_production_researcher',
    response_model=list[SoftwareProduction],
)
async def list_software_production(
    default_filters: DefaultFilters = Depends(),
    page: int | None = None,
    lenght: int | None = None,
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.list_software(
        conn, default_filters, page, lenght
    )


@router.get(
    '/bibliographic_production_article',
    response_model=list[ArticleProduction],
)
async def list_article_production(
    default_filters: DefaultFilters = Depends(),
    terms: str | None = None,
    qualis: str | None = None,
    page: int | None = None,
    lenght: int | None = None,
    conn: Connection = Depends(get_conn),
):
    if terms:
        default_filters.term = terms
    return await ProductionService.list_article_production(
        conn, default_filters, qualis, page, lenght
    )


@router.get(
    '/guidance_researcher',
    response_model=list[GuidanceProduction],
)
async def list_guidance_production(
    default_filters: DefaultFilters = Depends(),
    page: int | None = None,
    lenght: int | None = None,
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.list_guidance_production(
        conn, default_filters, page, lenght
    )


@router.get(
    '/researcher_production/events',
    response_model=list[EventProduction],
)
async def list_researcher_production_events(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.list_researcher_production_events(
        conn, default_filters
    )


@router.get(
    '/researcher_research_project',
    response_model=list[ResearchProjectProduction],
)
async def list_research_project(
    default_filters: DefaultFilters = Depends(),
    page: int | None = None,
    lenght: int | None = None,
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.list_research_projects(
        conn, default_filters, page, lenght
    )


@router.get(
    '/researcher_production/papers_magazine',
    response_model=list[PapersProduction],
)
async def list_papers_magazine(
    default_filters: DefaultFilters = Depends(),
    page: int | None = None,
    lenght: int | None = None,
    conn: Connection = Depends(get_conn),
):
    return await ProductionService.list_papers_magazine(
        conn, default_filters, page, lenght
    )
