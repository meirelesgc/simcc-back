from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.services import institution_service

router = APIRouter(prefix='/institution')


@router.get('/')
async def list_institutions(
    conn: Connection = Depends(get_conn),
):
    institution = await institution_service.get_institution(conn, None)
    if institution is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Institution not found.',
        )
    return institution


@router.get('/{institution_id}/')
async def get_institution(
    institution_id: UUID,
    conn: Connection = Depends(get_conn),
):
    institution = await institution_service.get_institution(conn, institution_id)
    if institution is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'Institution with ID {institution_id} not found.',
        )
    return institution
