import pytest
from http import HTTPStatus
from uuid import uuid4
from tests.factories import (
    BibliographicProductionFactory,
    InstitutionFactory,
    ResearcherFactory,
)
from simcc.core.db.model import OpenAlexArticle

@pytest.mark.asyncio
async def test_co_authorship_bibliographic_production(client, session):
    # Setup researchers from same institution
    inst = InstitutionFactory.build(name="Institution A")
    session.add(inst)
    await session.flush()
    
    res1 = ResearcherFactory.build(name="Alice Smith", institution_id=inst.id)
    res2 = ResearcherFactory.build(name="Bob Jones", institution_id=inst.id)
    session.add_all([res1, res2])
    await session.flush()
    
    # Production with same title for both
    title = "Collaborative Research"
    bp1 = BibliographicProductionFactory.build(researcher_id=res1.id, title=title)
    bp2 = BibliographicProductionFactory.build(researcher_id=res2.id, title=title)
    session.add_all([bp1, bp2])
    await session.commit()
    
    response = client.get(f"/researcher/co-authorship/{res1.id}")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    
    assert len(data) == 1
    assert data[0]['name'] == "Bob Jones"
    assert data[0]['type'] == "internal"
    assert data[0]['among'] == 1
    assert data[0]['initials'] == "BJ"

@pytest.mark.asyncio
async def test_co_authorship_openalex(client, session):
    inst = InstitutionFactory.build(name="Institution B")
    session.add(inst)
    await session.flush()
    
    res = ResearcherFactory.build(name="Charlie Brown", institution_id=inst.id)
    session.add(res)
    await session.flush()
    
    bp = BibliographicProductionFactory.build(researcher_id=res.id)
    session.add(bp)
    await session.flush()
    
    oa = OpenAlexArticle(
        id=uuid4(),
        article_id=bp.id,
        authors="Charlie Brown; David Green",
        authors_institution="Institution B; Harvard",
    )
    session.add(oa)
    await session.commit()
    
    response = client.get(f"/researcher/co-authorship/{res.id}")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    
    # It should find David Green
    assert any(item['name'] == "David Green" for item in data)
    assert not any(item['name'] == "Charlie Brown" for item in data)
    
    external = next(item for item in data if item['name'] == "David Green")
    assert external['type'] == "external"
    assert external['initials'] == "DG"

@pytest.mark.asyncio
async def test_co_authorship_empty(client, session):
    res_id = uuid4()
    response = client.get(f"/researcher/co-authorship/{res_id}")
    assert response.status_code == HTTPStatus.OK
    assert response.json() == []
