import pytest


@pytest.mark.asyncio
async def test_session(client, conn):
    assert await conn.select('SELECT TRUE', None, True)
