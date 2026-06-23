from http import HTTPStatus
from io import BytesIO
from uuid import uuid4

import polars as pl
import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_get_technician_ufmg(client, session):
    tech_id = uuid4()
    await session.execute(
        text("""
        INSERT INTO ufmg.technician (technician_id, full_name, work_regime)
        VALUES (:id, 'Tech One', '40h')
    """),
        {'id': tech_id},
    )
    await session.commit()

    response = client.get('/ufmg/technician')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) >= 1
    assert any(t['full_name'] == 'Tech One' for t in data)


@pytest.mark.asyncio
async def test_get_departament_ufmg(client, session):
    await session.execute(
        text("""
        INSERT INTO ufmg.departament (dep_id, dep_nom, dep_sigla)
        VALUES ('DEP001', 'Departamento de Teste', 'DT')
    """)
    )
    await session.commit()

    response = client.get('/ufmg/departament', params={'dep_id': 'DEP001'})
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    assert data[0]['dep_nom'] == 'Departamento de Teste'


@pytest.mark.asyncio
async def test_get_researcher_ufmg_name(client, session):
    from simcc.queries.external_query import ResearcherDataQuery

    await session.execute(
        text("""
        INSERT INTO ufmg.researcher_data (cpf, nome, classe, nivel)
        VALUES ('11122233344', 'Researcher Name Test', 1, 1)
    """)
    )
    await session.commit()

    # Test query object directly
    query = ResearcherDataQuery(session, name='Researcher Name Test')
    direct_data = await query.execute()
    assert len(direct_data) == 1, (
        f'Direct query failed, got {len(direct_data)} results'
    )

    # Try getting everything first
    response_all = client.get('/ufmg/researcher')
    assert response_all.status_code == HTTPStatus.OK
    data_all = response_all.json()
    assert len(data_all) >= 1, (
        'Should return at least one researcher when no filter applied'
    )

    # Exact match to avoid any potential ILIKE issue during debug
    response = client.get(
        '/ufmg/researcher', params={'name': 'Researcher Name Test'}
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    assert data[0]['nome'] == 'Researcher Name Test'


@pytest.mark.asyncio
async def test_get_docentes_ufmg(client, session):
    res_id = uuid4()
    await session.execute(
        text("""
        INSERT INTO ufmg.researcher (researcher_id, full_name, academic_degree)
        VALUES (:id, 'Docente Test', 'Doutorado')
    """),
        {'id': res_id},
    )
    await session.commit()

    response = client.get('/ufmg/docentes', params={'graduation': 'Doutorado'})
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    assert data[0]['full_name'] == 'Docente Test'


@pytest.mark.asyncio
async def test_list_article_production_ufmg(client, session):
    res_id = uuid4()
    await session.execute(
        text("""
        INSERT INTO researcher (id, name, lattes_id, status)
        VALUES (:id, 'Researcher A', '12345', true)
    """),
        {'id': res_id},
    )

    await session.execute(
        text("""
        INSERT INTO ufmg.departament_researcher (dep_id, researcher_id)
        VALUES ('DEP1', :id)
    """),
        {'id': res_id},
    )

    bp_id = uuid4()
    await session.execute(
        text("""
        INSERT INTO bibliographic_production (id, researcher_id, title, year_, type)
        VALUES (:id, :res_id, 'Article Test', '2021', 'ARTICLE')
    """),
        {'id': bp_id, 'res_id': res_id},
    )

    mag_id = uuid4()
    await session.execute(
        text("""
        INSERT INTO periodical_magazine (id, name)
        VALUES (:id, 'Nature')
    """),
        {'id': mag_id},
    )

    await session.execute(
        text("""
        INSERT INTO bibliographic_production_article (id, bibliographic_production_id, qualis, periodical_magazine_id)
        VALUES (:id, :bp_id, 'A1', :mag_id)
    """),
        {'id': uuid4(), 'bp_id': bp_id, 'mag_id': mag_id},
    )

    await session.commit()

    response = client.get(
        '/ufmg/departament/DEP1/article_production', params={'year': 2020}
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    assert data[0]['name'] == 'Researcher A'
    assert data[0]['a1'] == 1


@pytest.mark.asyncio
async def test_post_congregation_ufmg(client, session, monkeypatch):
    # Mock polars read_excel to avoid dependency in tests
    mock_data = pl.DataFrame({
        'MEMBRO': ['Membro Teste'],
        'DEPARTAMENTO': ['Depto Teste'],
        'MANDATO': ['2024-2026'],
        'E-MAIL': ['teste@ufmg.br'],
        'TELEFONE': ['1234-5678'],
    })

    def mock_read_excel(*args, **kwargs):
        return mock_data

    monkeypatch.setattr(pl, 'read_excel', mock_read_excel)

    excel_file = BytesIO(b'fake excel content')

    response = client.post(
        '/ufmg/congregation',
        files={
            'file': (
                'test.xlsx',
                excel_file,
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            )
        },
    )

    assert response.status_code == 200
    assert (
        response.json()['detail']
        == 'Congregation mandates processed successfully'
    )

    result = await session.execute(
        text("SELECT count(*) FROM ufmg.mandate WHERE member = 'Membro Teste'")
    )
    assert result.scalar() == 1


@pytest.mark.asyncio
async def test_second_word_endpoint(client, session):
    # Setup test data
    res_id = uuid4()
    lattes_id = str(uuid4())[:10]  # Ensure unique lattes_id
    await session.execute(
        text("""
        INSERT INTO researcher (id, name, lattes_id, status)
        VALUES (:id, 'Researcher A', :lattes_id, true)
    """),
        {'id': res_id, 'lattes_id': lattes_id},
    )

    # Word 'Artificial' (length > 3, not a stopword)
    # Word 'Intelligence'
    await session.execute(
        text("""
        INSERT INTO bibliographic_production (id, researcher_id, title, year_, type)
        VALUES (:id1, :res_id, 'Artificial Intelligence in Medicine', '2021', 'ARTICLE'),
               (:id2, :res_id, 'Artificial Neural Networks', '2022', 'ARTICLE')
    """),
        {'id1': uuid4(), 'id2': uuid4(), 'res_id': res_id},
    )
    await session.commit()

    # Search for words starting with 'art'
    response = client.get('/secondWord', params={'term': 'art'})
    assert response.status_code == HTTPStatus.OK
    data = response.json()

    assert len(data) >= 1
    # 'Artificial' appears twice
    assert any(item['word'] == 'artificial' and item['freq'] == 2 for item in data)
    
    # Search for words starting with 'neu'
    response = client.get('/secondWord', params={'term': 'neu'})
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert any(item['word'] == 'neural' and item['freq'] == 1 for item in data)

    # 'In' (stopword) should not appear even if searched for
    response = client.get('/secondWord', params={'term': 'in'})
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert not any(item['word'] == 'in' for item in data)
