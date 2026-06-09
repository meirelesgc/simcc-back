from http import HTTPStatus

import pytest

from tests.factories import (
    BibliographicProductionFactory,
    CityFactory,
    CountryFactory,
    GraduateProgramFactory,
    GraduateProgramResearcherFactory,
    InstitutionFactory,
    ResearcherFactory,
)


@pytest.mark.asyncio
async def test_get_graduate_program_production(client, session):
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

    gp = GraduateProgramFactory(institution_id=inst.id)
    session.add(gp)
    await session.flush()

    res = ResearcherFactory(
        institution_id=inst.id, city_id=city.id, graduation='DOUTORADO'
    )
    session.add(res)
    await session.flush()

    gpr = GraduateProgramResearcherFactory(
        graduate_program_id=gp.graduate_program_id, researcher_id=res.id
    )
    session.add(gpr)
    await session.flush()

    # Add production
    bp = BibliographicProductionFactory(
        researcher_id=res.id,
        title='Valid Production',
        type='ARTICLE',
        year_=2022,
    )
    session.add(bp)

    await session.commit()

    response = client.get(
        f'/production/graduate-program?graduate_program_id={gp.graduate_program_id}&year=2020'
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    assert data[0]['article'] >= 1
    assert data[0]['doctors'] >= 1
    assert data[0]['researcher'] >= 1


@pytest.mark.asyncio
async def test_get_graduate_program_production_by_dep(client, session):
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

    # We won't link to a program, we'll use researcher production directly
    res = ResearcherFactory(
        institution_id=inst.id, city_id=city.id, graduation='MESTRADO'
    )
    session.add(res)
    await session.flush()

    # Add production
    bp = BibliographicProductionFactory(
        researcher_id=res.id, title='Dep Production', type='BOOK', year_=2022
    )
    session.add(bp)

    await session.commit()

    # Testing without program_id (should count all or use dep_id if we had mocked ufmg.departament_researcher)
    response = client.get('/production/graduate-program?year=2020')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    assert data[0]['book'] >= 1
    assert data[0]['masters'] >= 1
