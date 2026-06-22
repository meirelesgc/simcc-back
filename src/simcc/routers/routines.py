from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException

from simcc.core.settings import Settings
from simcc.services import routines_service

router = APIRouter(tags=['Routines'])
SETTINGS = Settings()


@router.post(
    '/routines/researcher/{researcher_id}', status_code=HTTPStatus.ACCEPTED
)
async def trigger_researcher_routines(
    researcher_id: UUID,
    background_tasks: BackgroundTasks,
    x_internal_key: str | None = Header(default=None),
):
    if (
        not SETTINGS.INTERNAL_API_KEY
        or x_internal_key != SETTINGS.INTERNAL_API_KEY
    ):
        raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED)
    background_tasks.add_task(
        routines_service.run_researcher_routines, str(researcher_id)
    )
    return {'researcher_id': str(researcher_id)}
