from http import HTTPStatus
from typing import Optional

import httpx
from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import OAuth2PasswordBearer
from fastapi.security.api_key import APIKeyHeader

from simcc.config import Settings
from simcc.core.connection import Connection
from simcc.core.database import get_admin_conn

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='/xpto', auto_error=False)
key_header = APIKeyHeader(name='X-API-KEY', auto_error=False)


async def get_current_key(
    api_key: str = Security(key_header),
    conn: Connection = Depends(get_admin_conn),
):
    if not api_key:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Could not validate credentials',
        )

    SCRIPT_SQL = """
        SELECT 1
        FROM keys
        WHERE key = %(key)s
        AND deleted_at IS NULL
        LIMIT 1;
    """
    valid = await conn.select(SCRIPT_SQL, {'key': api_key}, one=True)
    if not valid:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='Invalid or inactive API key',
        )


async def get_current_user(
    request: Request, token: Optional[str] = Security(oauth2_scheme)
):
    if not token:
        token = request.cookies.get('Authorization')
        if not token:
            return None
        token = token.replace('Bearer ', '', 1)
    async with httpx.AsyncClient() as client:
        response = await client.get(Settings().ADMIN_URL)
        if response.status_code == HTTPStatus.OK:
            return response.json()
