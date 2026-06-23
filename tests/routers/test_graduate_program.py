import pytest

from tests.factories import (
    GraduateProgramFactory,
    InstitutionFactory,
)


@pytest.mark.asyncio
async def test_list_graduate_programs_profnit(client, session):
    # Setup data
    inst = InstitutionFactory.build(name='Universidade Teste')
    session.add(inst)
    await session.flush()

    gp = GraduateProgramFactory.build(
        name='Programa de Teste', institution_id=inst.id
    )
    session.add(gp)
    await session.commit()

    # Request
    response = client.get('/graduate_program_profnit')

    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert data[0]['name'] == 'Programa de Teste'
    assert data[0]['institution'] == 'Universidade Teste'


@pytest.mark.asyncio
async def test_graduate_program_validation_messy_data(client, session):
    from datetime import date
    from uuid import uuid4

    inst = InstitutionFactory.build()
    session.add(inst)
    await session.flush()

    gp_id = uuid4()
    from sqlalchemy import text

    await session.execute(
        text(
            'INSERT INTO graduate_program (graduate_program_id, name, area, modality, cooperation_project, start, institution_id) '
            'VALUES (:id, :name, :area, :modality, :coop, :start, :inst_id)'
        ),
        {
            'id': gp_id,
            'name': 'Programa Bagunçado',
            'area': 'TESTE',
            'modality': 'TESTE',
            'coop': 'Presente',
            'start': date(1995, 1, 1),
            'inst_id': inst.id,
        },
    )
    await session.commit()

    response = client.get(
        '/graduate_program_profnit', params={'graduate_program_id': str(gp_id)}
    )

    assert response.status_code == 200
    data = response.json()
    assert data[0]['cooperation_project'] is True
    assert data[0]['start'] == '1995-01-01'


@pytest.mark.asyncio
async def test_list_research_lines(client, session):
    from simcc.core.db.model import ResearchLinesPrograms

    inst = InstitutionFactory.build()
    session.add(inst)
    await session.flush()
    gp = GraduateProgramFactory.build(institution_id=inst.id)
    session.add(gp)
    await session.flush()

    line = ResearchLinesPrograms(
        graduate_program_id=gp.graduate_program_id,
        name='Linha de IA',
        area='Computação',
        start_year=2010,
        end_year=2024,
    )
    session.add(line)
    await session.commit()

    response = client.get('/graduate_program/lines', params={'term': 'IA'})
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]['name'] == 'Linha de IA'


@pytest.mark.asyncio
async def test_graduate_program_article_production(client, session):
    from tests.factories import (
        ArticleFactory,
        BibliographicProductionFactory,
        GraduateProgramResearcherFactory,
        ResearcherFactory,
    )

    inst = InstitutionFactory.build()
    session.add(inst)
    await session.flush()
    gp = GraduateProgramFactory.build(institution_id=inst.id)
    session.add(gp)
    await session.flush()

    res = ResearcherFactory.build(institution_id=inst.id)
    session.add(res)
    await session.flush()

    gpr = GraduateProgramResearcherFactory.build(
        graduate_program_id=gp.graduate_program_id,
        researcher_id=res.id,
        type_='PERMANENTE',
    )
    session.add(gpr)
    await session.flush()

    bp = BibliographicProductionFactory.build(
        researcher_id=res.id, year_='2022', type='ARTICLE'
    )
    session.add(bp)
    await session.flush()

    from tests.factories import PeriodicalMagazineFactory

    mag = PeriodicalMagazineFactory.build()
    session.add(mag)
    await session.flush()

    art = ArticleFactory.build(
        bibliographic_production_id=bp.id,
        periodical_magazine_id=mag.id,
        qualis='A1',
    )
    session.add(art)
    await session.commit()

    response = client.get(
        f'/graduate_program/{gp.graduate_program_id}/article_production',
        params={'year': 2020},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]['a1'] == 1
    assert data[0]['name'] == res.name


@pytest.mark.asyncio
async def test_list_graduate_program_researcher(client, session):
    from tests.factories import (
        GraduateProgramResearcherFactory,
        ResearcherFactory,
    )

    inst = InstitutionFactory.build()
    session.add(inst)
    await session.flush()
    gp = GraduateProgramFactory.build(institution_id=inst.id)
    session.add(gp)
    await session.flush()

    res = ResearcherFactory.build(institution_id=inst.id)
    session.add(res)
    await session.flush()

    gpr = GraduateProgramResearcherFactory.build(
        graduate_program_id=gp.graduate_program_id,
        researcher_id=res.id,
        type_='PERMANENTE',
        year=2020,
    )
    session.add(gpr)
    await session.commit()

    response = client.get(
        '/graduate_program_researcher',
        params={'graduate_program_id': str(gp.graduate_program_id)},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]['name'] == res.name
    assert data[0]['type'] == 'PERMANENTE'
