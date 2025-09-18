from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


class Connection:
    def __init__(self, conninfo: str, **kwargs):
        self.pool = AsyncConnectionPool(
            conninfo=conninfo,
            open=False,
            kwargs={'row_factory': dict_row},
            **kwargs,
        )

    async def connect(self):
        await self.pool.open(wait=True, timeout=self.pool.timeout)

    async def disconnect(self):
        await self.pool.close()

    async def exec(
        self,
        query: str,
        params: dict | None = None,
    ):
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.execute(query, params)

    async def execmany(
        self,
        query: str,
        params: list[dict] | None = None,
    ):
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.executemany(query, params)

    async def select(
        self,
        query: str,
        params: dict | None = None,
        one: bool = False,
    ):
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                if one:
                    return await cur.fetchone()
                return await cur.fetchall()
