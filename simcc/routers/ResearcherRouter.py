from uuid import UUID

from fastapi import APIRouter

from simcc.schemas import ResearcherOptions
from simcc.schemas.Researcher import (
    CoAuthorship,
    ProfessionalExperience,
    Researcher,
)
from simcc.services import ResearcherService

router = APIRouter()


@router.get(
    '/researcher',
    response_model=list[Researcher],
)
def search_in_abstract_or_article(
    terms: str = None,
    type: ResearcherOptions = 'ABSTRACT',
    graduate_program_id: UUID | str = None,
    university: str = None,
    group_id: UUID | str = None,
    page: int = None,
    lenght: int = None,
):
    if type == 'ARTICLE':
        return ResearcherService.search_in_articles(
            terms, graduate_program_id, university, group_id, page, lenght
        )
    elif type == 'ABSTRACT':
        return ResearcherService.search_in_abstracts(
            terms, graduate_program_id, university, group_id, page, lenght
        )


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
    lattes_id: str = None,
    dep_id: str = None,
    page: int = None,
    lenght: int = None,
):
    return ResearcherService.serch_in_name(
        name, graduate_program_id, dep_id, page, lenght, lattes_id
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
