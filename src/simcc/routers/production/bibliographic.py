from fastapi import APIRouter

from simcc.core.dependencies import (
    AdminAsyncSession,
    AsyncSession,
    CurrentUser,
    Filters,
)
from simcc.schemas import DefaultFilters, QualisOptions
from simcc.schemas.production import (
    ArticleProduction,
    BookChapterProduction,
    BookProduction,
    PapersProduction,
)
from simcc.services import production_service

router = APIRouter(tags=['Production - Bibliographic'])


@router.get('/production/book', response_model=list[BookProduction])
@router.get('/book_production_researcher', include_in_schema=False)
async def list_book_production(
    session: AsyncSession,
    admin_session: AdminAsyncSession,
    current_user: CurrentUser,
    filters: Filters,
    terms: str | None = None,
):
    filters.term = terms if terms else filters.term
    filters.star = current_user if filters.star else None

    return await production_service.list_book(session, admin_session, filters)


@router.get(
    '/production/book-chapter',
    response_model=list[BookChapterProduction],
)
@router.get(
    '/book_chapter_production_researcher',
    include_in_schema=False,
)
async def list_book_chapter_production(
    session: AsyncSession,
    admin_session: AdminAsyncSession,
    current_user: CurrentUser,
    filters: Filters,
    terms: str | None = None,
):
    filters.term = terms if terms else filters.term
    filters.star = current_user if filters.star else None

    return await production_service.list_book_chapter(
        session, admin_session, filters
    )


@router.get('/outstanding_articles', include_in_schema=False)
async def list_outstanding_articles(
    session: AsyncSession,
):
    articles = await production_service.list_bibliographic_production(
        session,
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
    session: AsyncSession,
    admin_session: AdminAsyncSession,
    current_user: CurrentUser,
    filters: Filters,
    terms: str | None = None,
    qualis: QualisOptions | str | None = None,
):
    filters.term = terms if terms else filters.term
    filters.type = 'ARTICLE'
    filters.star = current_user if filters.star else None

    return await production_service.list_bibliographic_production(
        session, admin_session, filters, qualis
    )


@router.get('/production/paper', response_model=list[PapersProduction])
@router.get('/researcher_production/papers_magazine', include_in_schema=False)
async def list_papers_magazine(
    session: AsyncSession,
    admin_session: AdminAsyncSession,
    current_user: CurrentUser,
    filters: Filters,
    terms: str | None = None,
):
    filters.term = terms if terms else filters.term
    filters.star = current_user if filters.star else None

    return await production_service.list_papers_magazine(
        session, admin_session, filters
    )
