from http import HTTPStatus

import pytest

from tests.factories import (
    BibliographicProductionFactory,
    CityFactory,
    CountryFactory,
    InstitutionFactory,
    ResearcherFactory,
)


@pytest.mark.asyncio
async def test_list_researcher_terms(client, session):
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

    # Add production with a specific title to generate terms
    bp = BibliographicProductionFactory(
        researcher_id=res.id,
        title='Artificial Intelligence in Medicine',
        type='ARTICLE',
    )
    session.add(bp)
    await session.commit()

    response = client.get(f'/researcher/terms?researcher_id={res.id}')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    # "Medicine", "Intelligence", "Artificial" should be in the terms (initcap)
    terms = [item['term'] for item in data]
    # Note: postgres unaccent/ts_stat might normalize differently,
    # but "Medicine" is a safe bet as it's > 3 chars and not a stopword.
    assert any('Medicin' in t or 'Medicine' in t for t in terms)
