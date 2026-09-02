import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

pytest_plugins = ['tests.ai.fixtures.ai_fixtures']

from simcc import app
from simcc.ai.dependencies import (
    get_embeddings_provider,
    get_llm_provider,
    get_query_planner,
)
from simcc.core.db.database import get_async_session
from simcc.core.db.models import table_registry
from simcc.core.settings import Settings


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'ai_live: marca testes que chamam a API real da OpenAI (gastam tokens e exigem OPENAI_API_KEY)',
    )


@pytest.fixture(scope='session')
def settings():
    return Settings()


from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


@pytest.fixture(scope='session')
def engine(settings):
    _engine = create_async_engine(
        settings.DATABASE_URL, poolclass=NullPool, echo=False
    )
    return _engine


@pytest_asyncio.fixture(scope='session', autouse=True)
async def setup_database(engine):
    try:
        async with engine.begin() as conn:
            await conn.execute(text('CREATE EXTENSION IF NOT EXISTS vector;'))
            await conn.run_sync(table_registry.metadata.create_all)
    except Exception:
        # Permite execução de testes puramente unitários sem conexão com banco
        pass


@pytest_asyncio.fixture
async def session(engine):
    """
    Inicia uma conexão e uma transação externa para o teste.
    Usa join_transaction_mode='create_savepoint' para que qualquer commit()
    feita pelo código sob teste ou pela fixture seja isolada em um SAVEPOINT.
    Ao final do teste, a transação externa é revertida (ROLLBACK),
    garantindo que NENHUM dado seja gravado permanentemente no banco.
    """
    connection = await engine.connect()
    trans = await connection.begin()

    async_session = AsyncSession(
        bind=connection,
        expire_on_commit=False,
        join_transaction_mode='create_savepoint',
    )

    try:
        yield async_session
    finally:
        await async_session.close()
        await trans.rollback()
        await connection.close()


@pytest.fixture
def client(
    mock_llm_provider, mock_embeddings_provider, mock_query_planner, engine
):
    """
    TestClient padrão para testes rápidos (0 tokens).
    Sobrescreve os providers de IA e usa sessão isolada para o TestClient.
    """

    async def get_session_override():
        async_session_factory = sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        async with async_session_factory() as s:
            yield s

    app.dependency_overrides[get_async_session] = get_session_override
    app.dependency_overrides[get_llm_provider] = lambda: mock_llm_provider
    app.dependency_overrides[get_embeddings_provider] = lambda: (
        mock_embeddings_provider
    )
    app.dependency_overrides[get_query_planner] = lambda: mock_query_planner

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def async_client(
    mock_llm_provider, mock_embeddings_provider, mock_query_planner, session
):
    """
    AsyncClient para testes HTTP assíncronos compartilhando a mesma transação isolada
    (com rollback automático) da fixture session.
    """
    from httpx import ASGITransport, AsyncClient

    async def get_session_override():
        yield session

    app.dependency_overrides[get_async_session] = get_session_override
    app.dependency_overrides[get_llm_provider] = lambda: mock_llm_provider
    app.dependency_overrides[get_embeddings_provider] = lambda: (
        mock_embeddings_provider
    )
    app.dependency_overrides[get_query_planner] = lambda: mock_query_planner

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url='http://test'
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
def live_client(settings, engine):
    """
    TestClient para testes E2E reais conectados à API da OpenAI.
    Sobrescreve apenas a sessão do banco de dados.
    """
    if not settings.OPENAI_API_KEY:
        pytest.skip('OPENAI_API_KEY não configurada')

    async def get_session_override():
        async_session_factory = sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        async with async_session_factory() as s:
            yield s

    app.dependency_overrides[get_async_session] = get_session_override

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()
