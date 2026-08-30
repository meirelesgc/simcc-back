from uuid import UUID

from fastapi import APIRouter, Query

from simcc.core.dependencies import AsyncSession
from simcc.schemas.institution import Institution, InstitutionMetric
from simcc.services import researcher_service

router = APIRouter(tags=['Institution'])


@router.get('/institution', response_model=list[Institution])
async def list_institutions(session: AsyncSession):
    return await researcher_service.list_institutions(session)


@router.get(
    '/institution/production-frequency', response_model=list[InstitutionMetric]
)
@router.get('/institutionFrequenci', include_in_schema=False)
async def list_institution_frequency(
    session: AsyncSession,
    terms: str | None = Query(None),
    university: str | None = Query(None),
    type: str = Query(...),
):
    return await researcher_service.list_institution_frequency(
        session, terms, university, type
    )


@router.get('/institution/{institution_id}', response_model=Institution)
async def get_institution(session: AsyncSession, institution_id: UUID):
    return await researcher_service.get_institution(session, institution_id)
