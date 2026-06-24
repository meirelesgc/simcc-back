from datetime import datetime

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from testcontainers.postgres import PostgresContainer

from simcc import app
from simcc.core.db.database import get_async_session, get_admin_async_session
from simcc.core.db.model import table_registry
from simcc.core.settings import Settings


@pytest.fixture
def client(session):
    def get_session_override():
        return session

    with TestClient(app) as client:
        app.dependency_overrides[get_async_session] = get_session_override
        app.dependency_overrides[get_admin_async_session] = get_session_override
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope='session')
def engine():
    # Caso do windows + Docker no CI
    import sys  # noqa: PLC0415

    if sys.platform == 'win32':
        yield create_async_engine(Settings().DATABASE_URL)

    else:
        with PostgresContainer('postgres:16', driver='psycopg') as postgres:
            _engine = create_async_engine(
                postgres.get_connection_url().replace(
                    'postgresql+psycopg2', 'postgresql+psycopg'
                )
            )
            yield _engine


@pytest_asyncio.fixture
async def session(engine):
    async with engine.begin() as conn:
        await conn.execute(text('CREATE EXTENSION IF NOT EXISTS unaccent'))
        await conn.execute(text('CREATE EXTENSION IF NOT EXISTS pg_trgm'))
        await conn.execute(text('CREATE SCHEMA IF NOT EXISTS logs'))
        await conn.execute(text('CREATE SCHEMA IF NOT EXISTS ufmg'))
        await conn.execute(text('CREATE SCHEMA IF NOT EXISTS admin'))
        await conn.execute(text('CREATE SCHEMA IF NOT EXISTS admin_ufmg'))
        await conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'routine_type') THEN
                    CREATE TYPE routine_type AS ENUM ('dim_titulacao', 'fat_area_specialty');
                END IF;
            END$$;
        """))
        await conn.execute(text('CREATE TABLE IF NOT EXISTS admin.researcher_area (researcher_id UUID, area_id UUID, focal_point BOOLEAN)'))
        await conn.execute(text('CREATE TABLE IF NOT EXISTS admin.areas (id UUID, name VARCHAR)'))
        await conn.execute(text('CREATE TABLE IF NOT EXISTS admin.guidance_co_supervisors (guidance_tracking_id UUID, co_supervisor_researcher_id UUID)'))
        await conn.execute(text('CREATE TABLE IF NOT EXISTS admin.guidance_tags (guidance_tracking_id UUID, tag_id UUID)'))
        await conn.execute(text('CREATE TABLE IF NOT EXISTS admin.tags (id UUID, name VARCHAR, color_code VARCHAR)'))
        await conn.execute(text('CREATE TABLE IF NOT EXISTS public.guidance_tracking (id UUID, student_researcher_id UUID, supervisor_researcher_id UUID, graduate_program_id UUID, deleted_at TIMESTAMP, start_date DATE, planned_date_project DATE, done_date_project DATE, planned_date_qualification DATE, done_date_qualification DATE, planned_date_conclusion DATE, done_date_conclusion DATE)'))
        await conn.run_sync(table_registry.metadata.create_all)

    async with engine.connect() as conn:
        transaction = await conn.begin()
        async with AsyncSession(
            bind=conn, expire_on_commit=False, join_transaction_mode='create_savepoint'
        ) as session:
            yield session
        await transaction.rollback()


def _mock_db_time(model, time: datetime):
    def fake_time_handler(mapper, connection, target):
        if hasattr(target, 'created_at'):
            target.created_at = time
        if hasattr(target, 'updated_at'):
            target.updated_at = time

    event.listen(model, 'before_insert', fake_time_handler)

    yield time

    event.remove(model, 'before_insert', fake_time_handler)


@pytest.fixture
def mock_db_time():
    return _mock_db_time
