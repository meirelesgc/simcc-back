import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from simcc.app import app
from simcc.core.connection import Connection
from simcc.core.database import get_conn


@pytest.fixture(scope='session', autouse=True)
def postgres():
    with PostgresContainer('pgvector/pgvector:pg17') as pg:
        yield pg


async def reset_database(conn: Connection):
    SCRIPT_SQL = """
        DROP SCHEMA IF EXISTS public CASCADE;
        DROP SCHEMA IF EXISTS logs CASCADE;
        DROP SCHEMA IF EXISTS ufmg CASCADE;
        DROP SCHEMA IF EXISTS embeddings CASCADE;

        CREATE SCHEMA public;
    """
    await conn.exec(SCRIPT_SQL)
    with open('init.sql', 'r', encoding='utf-8') as buffer:
        await conn.exec(buffer.read())


@pytest_asyncio.fixture
async def conn(postgres):
    connection_url = (
        f'postgresql://{postgres.username}:{postgres.password}'
        f'@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}'
        f'/{postgres.dbname}'
    )
    conn = Connection(connection_url, max_size=20, timeout=10)
    await conn.connect()
    await reset_database(conn)
    yield conn
    await conn.disconnect()


@pytest.fixture
def client(conn):
    async def get_conn_override():
        yield conn

    app.dependency_overrides[get_conn] = get_conn_override
    return TestClient(app)
