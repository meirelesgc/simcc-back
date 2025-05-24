from uuid import UUID

from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.schemas import ResearcherOptions
from simcc.schemas.Researcher import (
    CoAuthorship,
    ProfessionalExperience,
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
    term: str = None,
    graduate_program_id: UUID | str = None,
    university: str = None,
    type: str = None,
    page: int = None,
    lenght: int = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    return ResearcherService.search_in_participation_event(
        term,
        graduate_program_id,
        university,
        type,
        page,
        lenght,
        city,
        area,
        modality,
        graduation,
    )


@router.get(
    '/researcherBook',
    response_model=list[Researcher],
)
def search_in_book(
    term: str = None,
    graduate_program_id: UUID | str = None,
    university: str = None,
    type: str = None,
    page: int = None,
    lenght: int = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    return ResearcherService.search_in_book(
        term,
        graduate_program_id,
        university,
        type,
        page,
        lenght,
        city,
        area,
        modality,
        graduation,
    )


@router.get(
    '/researcher',
    response_model=list[Researcher],
)
def search_in_abstract_or_article(
    terms: str = None,
    type: ResearcherOptions = 'ABSTRACT',
    graduate_program_id: UUID | str = None,
    university: str = None,
    page: int = None,
    lenght: int = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    if type == 'ARTICLE':
        researchers = ResearcherService.search_in_articles(
            terms,
            graduate_program_id,
            university,
            page,
            lenght,
            city,
            area,
            modality,
            graduation,
        )
    elif type == 'ABSTRACT':
        researchers = ResearcherService.search_in_abstracts(
            terms,
            graduate_program_id,
            university,
            page,
            lenght,
            city,
            area,
            modality,
            graduation,
        )
    return researchers


@router.get(
    '/researcher/foment',
    response_model=list[Researcher],
)
def list_foment_researchers():
    return ResearcherService.list_foment_researchers()


@router.get(
    '/researcherName',
    response_model=list[Researcher],
)
def list_researchers(
    name: str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    page: int = None,
    lenght: int = None,
    area: str = None,
    graduate_program: str = None,
    city: str = None,
    institution: str = None,
    modality: str = None,
    graduation: str = None,
):
    return ResearcherService.serch_in_name(
        name,
        graduate_program_id,
        dep_id,
        page,
        lenght,
        area,
        graduate_program,
        city,
        institution,
        modality,
        graduation,
    )


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
    university: str = None,
    page: int = None,
    lenght: int = None,
):
    return ResearcherService.search_in_patents(
        term, graduate_program_id, university, page, lenght
    )


@router.get(
    '/researcher/co-authorship/{researcher_id}',
    response_model=list[CoAuthorship],
)
def co_authorship(researcher_id: UUID):
    return ResearcherService.list_co_authorship(researcher_id)


@router.get(
    '/professional_experience',
    response_model=list[ProfessionalExperience],
)
def get_professional_experience(
    researcher_id: UUID = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    year: int = None,
    page: int = None,
    lenght: int = None,
):
    return ResearcherService.professional_experience(
        researcher_id, graduate_program_id, dep_id, year, page, lenght
    )
