import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from simcc.app import app
from simcc.core.connection import Connection
from simcc.core.database import get_conn

POSTGRES_IMAGE = 'pgvector/pgvector:pg17'
INIT_SQL_FILE = 'init.sql'
DB_CONN_MAX_SIZE = 20
DB_CONN_TIMEOUT = 10
INIT_PATH = 'init.sql'


@pytest.fixture(scope='session', autouse=True)
def postgres():
    with PostgresContainer('pgvector/pgvector:pg17') as pg:
        yield pg


async def restore_db(conn: Connection):
    SCRIPT_SQL = """
        DROP SCHEMA IF EXISTS public CASCADE;
        DROP SCHEMA IF EXISTS logs CASCADE;
        DROP SCHEMA IF EXISTS ufmg CASCADE;
        DROP SCHEMA IF EXISTS embeddings CASCADE;

        CREATE SCHEMA public;
    """
    await conn.exec(SCRIPT_SQL)

    with open(INIT_PATH, 'r', encoding='utf-8') as buffer:
        await conn.exec(buffer.read())


@pytest_asyncio.fixture
async def conn(postgres):
    conn = Connection(
        f'postgresql://{postgres.username}:{postgres.password}'
        f'@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}'
        f'/{postgres.dbname}',
        max_size=DB_CONN_MAX_SIZE,
        timeout=DB_CONN_TIMEOUT,
    )

    await conn.connect()

    await restore_db(conn)

    yield conn

    await conn.disconnect()


@pytest.fixture
def client(conn: Connection):
    async def get_conn_override():
        yield conn

    app.dependency_overrides[get_conn] = get_conn_override

    yield TestClient(app)

    app.dependency_overrides.clear()
