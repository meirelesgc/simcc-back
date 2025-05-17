from uuid import UUID

from fastapi import APIRouter, Depends

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.schemas.GraduateProgram import GraduateProgram
from simcc.schemas.Researcher import ResearcherArticleProduction
from simcc.services import GraduateProgramService

router = APIRouter()


@router.get(
    '/graduate_program/',
    response_model=list[GraduateProgram],
)
async def list_graduate_programs(
    graduate_program_id: UUID | str = None,
    university: str = None,
    conn: Connection = Depends(get_conn),
):
    return await GraduateProgramService.list_graduate_programs(
        conn, graduate_program_id, university
    )


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
