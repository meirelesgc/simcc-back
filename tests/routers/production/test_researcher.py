from http import HTTPStatus

import pytest

from simcc import app
from simcc.core.security import get_current_user
from tests.factories import create_researcher_with_full_graph


@pytest.fixture(autouse=True)
def override_current_user():
    app.dependency_overrides[get_current_user] = lambda: {'id': 'dummy'}
    yield
    app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_search_researchers_semantic_term(client, session):
    # Setup: Create a researcher with specific abstract
    researcher = await create_researcher_with_full_graph(session)

    # Test: Search for "machine learning" which is in the abstract
    # We also filter by researcher_id to avoid pollution from other tests in the same session
    response = client.get(
        '/researchers',
        params={
            'term': 'machine learning',
            'type': 'ABSTRACT',
            'researcher_id': str(researcher.id),
        },
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    assert data[0]['id'] == str(researcher.id)
    assert 'machine learning' in data[0]['abstract'].lower()


@pytest.mark.asyncio
async def test_search_researchers_by_city(client, session):
    researcher = await create_researcher_with_full_graph(session)
    # We need to fetch the city name
    from sqlalchemy import select

    from simcc.core.db.model import City

    res = await session.execute(
        select(City).where(City.id == researcher.city_id)
    )
    city = res.scalar_one()

    response = client.get('/researchers/by-city', params={'city': city.name})

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['id'] == str(researcher.id)
    assert data[0]['city'] == city.name


@pytest.mark.asyncio
async def test_search_researchers_pagination(client, session):
    await create_researcher_with_full_graph(session)
    await create_researcher_with_full_graph(session)

    # Test limit 1
    response = client.get('/researchers', params={'lenght': 1})
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()) == 1

    # Test offset 1
    response = client.get('/researchers', params={'page': 2, 'lenght': 1})
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()) == 1
