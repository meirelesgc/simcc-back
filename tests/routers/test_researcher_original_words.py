from http import HTTPStatus

import pytest

from simcc.core.db.model import (
    AreaExpertise,
    AreaSpecialty,
    GreatAreaExpertise,
    ResearchDictionary,
    SubAreaExpertise,
)
from tests.factories import ResearcherFactory


@pytest.mark.asyncio
async def test_originals_words_name(client, session):
    res = ResearcherFactory(name='Alan Turing')
    session.add(res)
    await session.commit()

    response = client.get(
        '/researchers/original-words', params={'initials': 'ala', 'type': 'name'}
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    # capitalize() on "Alan Turing" results in "Alan turing"
    assert data[0]['term'] == 'Alan turing'
    assert data[0]['type'] == '0'


@pytest.mark.asyncio
async def test_originals_words_area(client, session):
    ga = GreatAreaExpertise(name='Exact Sciences')
    session.add(ga)
    await session.flush()

    ae = AreaExpertise(name='Computer Science', great_area_expertise_id=ga.id)
    session.add(ae)
    await session.flush()

    sae = SubAreaExpertise(
        name='Artificial Intelligence', area_expertise_id=ae.id
    )
    session.add(sae)
    await session.flush()

    asp = AreaSpecialty(name='Machine Learning', sub_area_expertise_id=sae.id)
    session.add(asp)
    await session.commit()

    response = client.get(
        '/researchers/original-words', params={'initials': 'mach', 'type': 'area'}
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['term'] == 'Machine learning'
    assert data[0]['type'] == 'AREA_SPECIALTY'


@pytest.mark.asyncio
async def test_originals_words_dictionary(client, session):
    rd = ResearchDictionary(
        term='Deep Learning', frequency=1, type_='ARTICLE'
    )
    session.add(rd)
    await session.commit()

    response = client.get(
        '/researchers/original-words',
        params={'initials': 'dee', 'type': 'ARTICLE'},
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['term'] == 'Deep learning'
    assert data[0]['type'] == 'ARTICLE'
    assert data[0]['frequency'] == '1'
