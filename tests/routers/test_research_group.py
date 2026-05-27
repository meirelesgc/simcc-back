import pytest
from uuid import uuid4

from tests.factories import InstitutionFactory


@pytest.mark.asyncio
async def test_list_research_groups(client, session):
    inst = InstitutionFactory.build(name='Universidade Federal', acronym='UF')
    session.add(inst)
    await session.flush()

    group_id = uuid4()
    # Inserindo manualmente pois não temos factory completa para ResearchGroup com todos os campos necessários
    from sqlalchemy import text
    await session.execute(text(
        "INSERT INTO research_group (id, name, institution, first_leader, first_leader_id, area) "
        "VALUES (:id, :name, :institution, :first_leader, :first_leader_id, :area)"
    ), {
        "id": group_id,
        "name": "Grupo de Teste",
        "institution": "UF",
        "first_leader": "Lider Teste",
        "first_leader_id": uuid4(),
        "area": "Exatas"
    })
    await session.commit()

    response = client.get('/research_group')
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert data[0]['name'] == "Grupo de Teste"
    assert "UF - Universidade Federal" in data[0]['institution']


@pytest.mark.asyncio
async def test_list_research_lines(client, session):
    group_id = uuid4()
    # Inserindo research_group simplificado
    from sqlalchemy import text
    await session.execute(text(
        "INSERT INTO research_group (id, name, institution, first_leader_id) "
        "VALUES (:id, :name, :institution, :first_leader_id)"
    ), {
        "id": group_id,
        "name": "Grupo para Linhas",
        "institution": "UF",
        "first_leader_id": uuid4()
    })
    
    await session.execute(text(
        "INSERT INTO research_lines (id, research_group_id, title, objective) "
        "VALUES (:id, :group_id, :title, :objective)"
    ), {
        "id": uuid4(),
        "group_id": group_id,
        "title": "Linha de Pesquisa 1",
        "objective": "Objetivo 1"
    })
    await session.commit()

    response = client.get('/research_group_lines', params={'group_id': str(group_id)})
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]['line'] == "Linha de Pesquisa 1"


@pytest.mark.asyncio
async def test_get_research_group_count(client, session):
    from sqlalchemy import text
    await session.execute(text(
        "INSERT INTO research_group (id, area, institution) VALUES (:id, :area, :institution)"
    ), {"id": uuid4(), "area": "Saude", "institution": "UF"})
    await session.execute(text(
        "INSERT INTO research_group (id, area, institution) VALUES (:id, :area, :institution)"
    ), {"id": uuid4(), "area": "Saude", "institution": "UF"})
    await session.commit()

    response = client.get('/research_group/count')
    assert response.status_code == 200
    data = response.json()
    
    # Encontrar a área "Saude" no retorno
    saude_count = next(item for item in data if item['area'] == 'Saude')
    assert saude_count['count'] >= 2
