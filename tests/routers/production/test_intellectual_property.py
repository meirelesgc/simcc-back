from http import HTTPStatus

import pytest

from simcc import app
from simcc.core.security import get_current_user
from tests.factories import (
    CityFactory,
    CountryFactory,
    InstitutionFactory,
    PatentFactory,
    ResearcherFactory,
    SoftwareFactory,
)


@pytest.fixture(autouse=True)
def override_current_user():
    app.dependency_overrides[get_current_user] = lambda: {'id': 'dummy'}
    yield
    app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_list_patent_production(client, session):
    # Setup data
    country = CountryFactory()
    session.add(country)
    await session.flush()

    inst = InstitutionFactory()
    session.add(inst)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    res = ResearcherFactory(institution_id=inst.id, city_id=city.id)
    session.add(res)
    await session.flush()

    patent = PatentFactory(researcher_id=res.id)
    session.add(patent)
    await session.commit()

    response = client.get('/production/patent')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['title'] == patent.title


@pytest.mark.asyncio
async def test_list_software_production(client, session):
    # Setup data
    country = CountryFactory()
    session.add(country)
    await session.flush()

    inst = InstitutionFactory()
    session.add(inst)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    res = ResearcherFactory(institution_id=inst.id, city_id=city.id)
    session.add(res)
    await session.flush()

    software = SoftwareFactory(researcher_id=res.id)
    session.add(software)
    await session.commit()

    response = client.get('/production/software')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['title'] == software.title
