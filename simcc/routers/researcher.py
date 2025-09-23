from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_admin_conn, get_conn
from simcc.schemas import DefaultFilters
from simcc.schemas.Researcher import (
    AcademicDegree,
    CoAuthorship,
    Researcher,
)
from simcc.security import get_current_user
from simcc.services import researcher_service

router = APIRouter()

Conn = Annotated[Connection, Depends(get_conn)]
AdminConn = Annotated[Connection, Depends(get_admin_conn)]

Filters = Annotated[DefaultFilters, Depends()]

CurrentUser = Annotated[dict, Depends(get_current_user)]


@router.get('/researcher_filter')
async def get_researcher_filter(conn: Conn):
    return await researcher_service.get_researcher_filter(conn)


@router.get('/researcherParticipationEvent', response_model=list[Researcher])
async def search_in_participation_event(
    current_user: CurrentUser,
    conn: Conn,
    conn_admin: AdminConn,
    filters: Filters,
):
    filters.star = current_user if filters.star else None
    return await researcher_service.search_in_participation_event(
        conn, conn_admin, filters
    )


@router.get(
    '/researcherArea_specialty',
    response_model=list[Researcher],
)
async def search_in_area_specialty(
    current_user: CurrentUser,
    conn: Conn,
    conn_admin: AdminConn,
    filters: Filters,
    area_specialty: str = None,
):
    filters.star = current_user if filters.star else None
    if area_specialty:
        filters.term = area_specialty
    return await researcher_service.search_in_area_specialty(
        conn, conn_admin, filters
    )


@router.get('/researcherBook', response_model=list[Researcher])
async def search_in_book(
    current_user: CurrentUser,
    conn: Conn,
    conn_admin: AdminConn,
    filters: Filters,
):
    filters.star = current_user if filters.star else None
    return await researcher_service.search_in_bibliographic_production(
        conn, conn_admin, filters, 'BOOK'
    )


@router.get('/researcher', response_model=list[Researcher])
async def search_in_abstract_or_article(
    current_user: CurrentUser,
    conn: Conn,
    conn_admin: AdminConn,
    filters: Filters,
    name: str = None,
):
    filters.star = current_user if filters.star else None
    if filters.type == 'ARTICLE':
        return await researcher_service.search_in_bibliographic_production(
            conn, conn_admin, filters, 'ARTICLE'
        )
    elif filters.type == 'ABSTRACT':
        return await researcher_service.search_in_researcher(
            conn, conn_admin, filters, name
        )


@router.get('/researcher/foment', response_model=list[Researcher])
async def list_foment_researchers(
    current_user: CurrentUser,
    conn: Conn,
    conn_admin: AdminConn,
    filters: Filters,
):
    filters.star = current_user if filters.star else None
    filters.modality = '*' if not filters.modality else filters.modality
    return await researcher_service.search_in_researcher(
        conn, conn_admin, filters, None
    )


@router.get('/researcherName', response_model=list[Researcher])
async def list_researchers(
    current_user: CurrentUser,
    conn: Conn,
    conn_admin: AdminConn,
    filters: Filters,
    name: str = None,
):
    filters.star = current_user if filters.star else None
    return await researcher_service.search_in_researcher(
        conn, conn_admin, filters, name
    )


@router.get('/outstanding_researchers', response_model=list[Researcher])
async def list_outstanding_researchers(conn: Conn, filters: Filters):
    filters.page = 0
    filters.lenght = 10
    return await researcher_service.list_outstanding_researchers(conn, filters)


@router.get('/researcherPatent', response_model=list[Researcher])
async def list_researchers_by_patent(
    current_user: CurrentUser,
    conn: Conn,
    conn_admin: AdminConn,
    filters: Filters,
):
    filters.star = current_user if filters.star else None
    return await researcher_service.search_in_patents(conn, conn_admin, filters)


@router.get(
    '/researcher/co-authorship/{researcher_id}',
    response_model=list[CoAuthorship],
)
def co_authorship(researcher_id: UUID):
    return researcher_service.list_co_authorship(researcher_id)


@router.get('/academic_degree', response_model=list[AcademicDegree])
async def get_academic_degree(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await researcher_service.get_academic_degree(conn, default_filters)
    return metrics


@router.get('/great_area')
async def get_great_area(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await researcher_service.get_great_area(conn, default_filters)
    return metrics


@router.get('/labs')
async def get_labs(
    conn: Conn, lattes_id: str = None, researcher_id: UUID | str = None
):
    return await researcher_service.get_labs(conn, lattes_id, researcher_id)
