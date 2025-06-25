from http import HTTPStatus

from fastapi import Depends, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader

from simcc.core.connection import Connection
from simcc.core.database import get_admin_conn

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
