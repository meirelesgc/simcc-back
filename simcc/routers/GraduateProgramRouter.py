from uuid import UUID

from fastapi import APIRouter

from simcc.schemas.GraduateProgram import GraduateProgram
from simcc.schemas.Researcher import ResearcherArticleProduction
from simcc.services import GraduateProgramService

router = APIRouter()


@router.get(
    '/graduate_program/',
    response_model=list[GraduateProgram],
)
def list_graduate_programs(
    graduate_program_id: UUID | str = None,
    university: str = None,
):
    graduate_programs = GraduateProgramService.list_graduate_programs(
        graduate_program_id, university
    )
    return graduate_programs


@router.get(
    '/graduate_program/{program_id}/article_production',
    response_model=list[ResearcherArticleProduction],
)
def article_production(program_id: UUID, year: int = 2020):
    return GraduateProgramService.list_article_production(program_id, year)


@router.get(
    '/graduate_program_profnit',
    response_model=list[GraduateProgram],
)
def list_programs(
    id: UUID | str = None,
    university: str = None,
):
    return GraduateProgramService.list_graduate_programs(id, university)
