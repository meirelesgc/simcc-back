from http import HTTPStatus
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from simcc import app
from simcc.core.db.database import get_async_session
from simcc.core.db.models.institution import Institution
from simcc.core.db.models.researcher import Researcher


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_researcher_image_no_params():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url='http://test'
    ) as ac:
        response = await ac.get('/researcher/image')
    assert response.status_code == HTTPStatus.BAD_REQUEST


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_researcher_image_non_existent_researcher(session):
    async def override_get_session():
        yield session

    app.dependency_overrides[get_async_session] = override_get_session
    try:
        non_existent_uuid = str(uuid4())
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url='http://test'
        ) as ac:
            response = await ac.get(
                f'/researcher/image?researcher_id={non_existent_uuid}'
            )
        assert response.status_code == HTTPStatus.NOT_FOUND
    finally:
        app.dependency_overrides.pop(get_async_session, None)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_researcher_image_fallback_to_default_avatar(session):
    async def override_get_session():
        yield session

    app.dependency_overrides[get_async_session] = override_get_session
    try:
        unique_acronym = f'U{uuid4().hex[:6].upper()}'
        inst = Institution(
            name=f'Universidade {unique_acronym}',
            acronym=unique_acronym,
        )
        session.add(inst)
        await session.commit()
        await session.refresh(inst)

        unique_lattes = str(uuid4().int)[:16]
        researcher = Researcher(
            name=f'Pesquisador Sem Foto {unique_lattes}',
            lattes_id=unique_lattes,
            lattes_10_id=None,
            institution_id=inst.id,
        )
        session.add(researcher)
        await session.commit()
        await session.refresh(researcher)

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url='http://test'
        ) as ac:
            response = await ac.get(
                f'/researcher/image?researcher_id={researcher.id}'
            )

        assert response.status_code == HTTPStatus.OK
        assert response.headers['content-type'] in (
            'image/png',
            'image/jpeg',
            'image/svg+xml',
        )
        assert len(response.content) > 0
    finally:
        app.dependency_overrides.pop(get_async_session, None)
