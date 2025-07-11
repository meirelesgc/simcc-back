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
from simcc.services import ResearcherService

router = APIRouter()


@router.get('/researcher_filter')
async def get_researcher_filter(
    conn: Connection = Depends(get_conn),
):
    return await ResearcherService.get_researcher_filter(conn)


@router.get(
    '/researcherParticipationEvent',
    response_model=list[Researcher],
)
def search_in_participation_event(
    type: str = None,
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
    return ResearcherService.search_in_participation_event(
        type,
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
    '/researcherArea_specialty',
    response_model=list[Researcher],
)
async def search_in_area_specialty(
    area_specialty: str = None,
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    if area_specialty:
        default_filters.term = area_specialty
    return await ResearcherService.search_in_area_specialty(
        conn, default_filters
    )


@router.get(
    '/researcherBook',
    response_model=list[Researcher],
)
def search_in_book(
    type: str = None,
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
    return ResearcherService.search_in_book(
        type,
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
    '/researcher',
    response_model=list[Researcher],
)
async def search_in_abstract_or_article(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    if default_filters.type == 'ARTICLE':
        return await ResearcherService.search_in_articles(conn, default_filters)
    elif default_filters.type == 'ABSTRACT':
        return await ResearcherService.search_in_abstracts(conn, default_filters)


@router.get('/researcher/foment', response_model=list[Researcher])
async def list_foment_researchers(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await ResearcherService.list_foment_researchers(default_filters, conn)


@router.get(
    '/researcherName',
    response_model=list[Researcher],
)
async def list_researchers(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
    name: str = None,
):
    return await ResearcherService.serch_in_name(conn, default_filters, name)


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
    return ResearcherService.list_outstanding_researchers(
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
    return ResearcherService.search_in_patents(
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
    return ResearcherService.list_co_authorship(researcher_id)


@router.get(
    '/academic_degree',
    response_model=list[AcademicDegree],
)
async def get_academic_degree(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ResearcherService.get_academic_degree(conn, default_filters)
    return metrics


@router.get(
    '/great_area',
)
async def get_great_area(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    metrics = await ResearcherService.get_great_area(conn, default_filters)
    return metrics
