from http import HTTPStatus

import pytest
from sqlalchemy import select

from simcc.core.db.model import Institution
from tests.factories import (
    ArticleFactory,
    BibliographicProductionFactory,
    InstitutionFactory,
    ResearcherFactory,
    create_researcher_with_full_graph,
)


@pytest.mark.asyncio
async def test_list_institution_frequency_article(client, session):
    # Setup researcher with full graph
    researcher = await create_researcher_with_full_graph(session)

    # Get institution name from DB
    result = await session.execute(
        select(Institution.name).where(Institution.id == researcher.institution_id)
    )
    institution_name = result.scalar_one()

    # Request frequency for "Machine" (from "Advanced Machine Learning Algorithms")
    response = client.get(
        '/institution/production-frequency',
        params={'terms': 'Machine', 'type': 'ARTICLE'}
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    data = response.json()
    assert len(data) > 0
    # Should find the institution
    institutions = [item['institution'] for item in data]
    assert institution_name in institutions


@pytest.mark.asyncio
async def test_list_institution_frequency_with_institution_filter(client, session):
    inst1 = InstitutionFactory(name='UFMG_TEST', acronym='UFMG')
    inst2 = InstitutionFactory(name='USP_TEST', acronym='USP')
    session.add_all([inst1, inst2])
    await session.flush()

    res1 = ResearcherFactory(
        institution_id=inst1.id, abstract='Bioinformatics study'
    )
    res2 = ResearcherFactory(
        institution_id=inst2.id, abstract='Bioinformatics study'
    )
    session.add_all([res1, res2])
    await session.commit()

    # Filter for UFMG_TEST only
    response = client.get(
        '/institution/production-frequency',
        params={
            'terms': 'Bioinformatics',
            'type': 'ABSTRACT',
            'university': 'UFMG_TEST',
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    data = response.json()
    assert len(data) == 1
    assert data[0]['institution'] == 'UFMG_TEST'
