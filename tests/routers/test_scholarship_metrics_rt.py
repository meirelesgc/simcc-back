from http import HTTPStatus
import pytest
from tests.factories import ResearcherFactory
from simcc.core.db.model import Foment
from sqlalchemy import text

@pytest.mark.asyncio
async def test_get_scholarship_metrics_empty(client, session):
    # Ensure empty
    await session.execute(text("DELETE FROM foment"))
    await session.execute(text("DELETE FROM researcher"))
    await session.commit()

    response = client.get('/metrics/researcher/scholarship')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0

@pytest.mark.asyncio
async def test_get_scholarship_metrics_with_data(client, session):
    # Ensure empty
    await session.execute(text("DELETE FROM foment"))
    await session.execute(text("DELETE FROM researcher"))
    await session.commit()

    # Create researchers
    r1 = ResearcherFactory()
    r2 = ResearcherFactory()
    r3 = ResearcherFactory()
    session.add_all([r1, r2, r3])
    await session.commit()
    
    # Create scholarships
    # r1 has PQ 1A
    f1 = Foment(researcher_id=r1.id, modality_code='PQ', category_level_code='1A')
    # r2 has DT 2
    f2 = Foment(researcher_id=r2.id, modality_code='DT', category_level_code='2')
    # r3 also has PQ 1A
    f3 = Foment(researcher_id=r3.id, modality_code='PQ', category_level_code='1A')
    
    session.add_all([f1, f2, f3])
    await session.commit()
    
    response = client.get('/metrics/researcher/scholarship')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 2 # PQ 1A and DT 2
    
    # Sort for stable testing
    data.sort(key=lambda x: x['modality_code'])
    
    # DT 2
    assert data[0]['modality_code'] == 'DT'
    assert data[0]['category_level_code'] == '2'
    assert data[0]['count'] == 1
    
    # PQ 1A
    assert data[1]['modality_code'] == 'PQ'
    assert data[1]['category_level_code'] == '1A'
    assert data[1]['count'] == 2

@pytest.mark.asyncio
async def test_get_scholarship_metrics_distinct_researchers(client, session):
    # Ensure empty
    await session.execute(text("DELETE FROM foment"))
    await session.execute(text("DELETE FROM researcher"))
    await session.commit()

    r1 = ResearcherFactory()
    session.add(r1)
    await session.commit()
    
    # Same researcher with two PQ 1A scholarships
    f1 = Foment(researcher_id=r1.id, modality_code='PQ', category_level_code='1A', call_title='Call 1')
    f2 = Foment(researcher_id=r1.id, modality_code='PQ', category_level_code='1A', call_title='Call 2')
    
    session.add_all([f1, f2])
    await session.commit()
    
    response = client.get('/metrics/researcher/scholarship')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    assert data[0]['modality_code'] == 'PQ'
    assert data[0]['category_level_code'] == '1A'
    assert data[0]['count'] == 1
