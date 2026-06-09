from http import HTTPStatus

import pytest

from tests.factories import (
    CityFactory,
    CountryFactory,
    InstitutionFactory,
    ResearcherFactory,
)


@pytest.mark.asyncio
async def test_list_institutions(client, session):
    # Setup data
    country = CountryFactory()
    session.add(country)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    inst = InstitutionFactory(acronym='TEST_INST')
    session.add(inst)
    await session.flush()

    res = ResearcherFactory(institution_id=inst.id, city_id=city.id)
    session.add(res)

    await session.commit()

    response = client.get('/institution')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert any(i['acronym'] == 'TEST_INST' for i in data)


@pytest.mark.asyncio
async def test_get_institution_by_id(client, session):
    # Setup data
    country = CountryFactory()
    session.add(country)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    inst = InstitutionFactory(acronym='UNIQUE_INST')
    session.add(inst)
    await session.flush()

    res = ResearcherFactory(
        institution_id=inst.id, city_id=city.id, lattes_id='LATTES_TEST_123'
    )
    session.add(res)

    await session.commit()

    response = client.get(f'/institution/{inst.id}')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['acronym'] == 'UNIQUE_INST'
    assert data['count_r'] >= 1
    assert 'LATTES_TEST_123' in data['researchers']
