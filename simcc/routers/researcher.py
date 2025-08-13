from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.schemas import DefaultFilters
from simcc.schemas.Researcher import (
    AcademicDegree,
    CoAuthorship,
    Researcher,
)
from simcc.services import researcher_service

router = APIRouter()

Conn = Annotated[Connection, Depends(get_conn)]
Filters = Annotated[DefaultFilters, Depends()]


@router.get('/researcher_filter')
async def get_researcher_filter(conn: Conn):
    return await researcher_service.get_researcher_filter(conn)


@router.get('/researcherParticipationEvent', response_model=list[Researcher])
async def search_in_participation_event(conn: Conn, filters: Filters):
    return await researcher_service.search_in_participation_event(conn, filters)


@router.get(
    '/researcherArea_specialty',
    response_model=list[Researcher],
)
async def search_in_area_specialty(
    conn: Conn, filters: Filters, area_specialty: str = None
):
    if area_specialty:
        filters.term = area_specialty
    return await researcher_service.search_in_area_specialty(conn, filters)


@router.get('/researcherBook', response_model=list[Researcher])
async def search_in_book(conn: Conn, filters: Filters):
    return await researcher_service.search_in_bibliographic_production(
        conn, filters, 'BOOK'
    )


@router.get('/researcher', response_model=list[Researcher])
async def search_in_abstract_or_article(
    conn: Conn, filters: Filters, name: str = None
):
    if filters.type == 'ARTICLE':
        return await researcher_service.search_in_bibliographic_production(
            conn, filters, 'ARTICLE'
        )
    elif filters.type == 'ABSTRACT':
        return await researcher_service.search_in_researcher(conn, filters, name)


@router.get('/researcher/foment', response_model=list[Researcher])
async def list_foment_researchers(conn: Conn, filters: Filters):
    if not filters.modality:
        filters.modality = '*'
    return await researcher_service.search_in_researcher(conn, filters, None)


@router.get('/researcherName', response_model=list[Researcher])
async def list_researchers(conn: Conn, filters: Filters, name: str = None):
    return await researcher_service.search_in_researcher(conn, filters, name)


@router.get(
    '/outstanding_researchers',
    response_model=list[Researcher],
)
def list_outstanding_researchers(
    name: str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    page: int = None,
    lenght: int = None,
):
    return researcher_service.list_outstanding_researchers(
        name, graduate_program_id, dep_id, page, lenght
    )


@router.get('/researcherPatent', response_model=list[Researcher])
def list_researchers_by_patent(
    term: str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    return researcher_service.search_in_patents(
        term,
        graduate_program_id,
        dep_id,
        departament,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
    )


@router.get(
    '/researcher/co-authorship/{researcher_id}',
    response_model=list[CoAuthorship],
)
def co_authorship(researcher_id: UUID):
    return researcher_service.list_co_authorship(researcher_id)


@router.get(
    '/academic_degree',
    response_model=list[AcademicDegree],
)
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
