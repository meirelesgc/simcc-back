from uuid import uuid4

import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_get_departament_rt(client, session):
    # Insert some data into ufmg.researcher and ufmg.technician
    # Note: Using text() for raw SQL because we are testing the query object's logic

    # We need to make sure the researcher_id exists if there are FKs,
    # but based on the schema, it's just a table in ufmg schema.

    await session.execute(
        text("""
        INSERT INTO ufmg.researcher (researcher_id, work_regime) 
        VALUES (:id1, 'DE'), (:id2, 'DE'), (:id3, '20h')
    """),
        {'id1': uuid4(), 'id2': uuid4(), 'id3': uuid4()},
    )

    await session.execute(
        text("""
        INSERT INTO ufmg.technician (technician_id, work_regime) 
        VALUES (:id1, '40h'), (:id2, '40h')
    """),
        {'id1': uuid4(), 'id2': uuid4()},
    )

    await session.commit()

    response = client.get('/departament/rt')

    assert response.status_code == 200
    data = response.json()

    assert 'teachers' in data
    assert 'technician' in data

    # Check teachers
    teachers = data['teachers']
    de_count = next(item for item in teachers if item['rt'] == 'DE')['count']
    h20_count = next(item for item in teachers if item['rt'] == '20h')['count']
    assert de_count == 2
    assert h20_count == 1

    # Check technician
    technicians = data['technician']
    h40_count = next(item for item in technicians if item['rt'] == '40h')[
        'count'
    ]
    assert h40_count == 2


@pytest.mark.asyncio
async def test_get_ufmg_departament_rt(client, session):
    # This path should also work and return the same structure
    response = client.get('/ufmg/departament/rt')
    assert response.status_code == 200
    data = response.json()
    assert 'teachers' in data
    assert 'technician' in data
