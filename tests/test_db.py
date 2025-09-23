import pytest


@pytest.mark.asyncio
async def test_session(client, conn):
    assert await conn.select('SELECT TRUE', None, True)


@pytest.mark.asyncio
async def test_create_country(conn, create_country):
    EXPECTED_COUNT = 1
    for _ in range(EXPECTED_COUNT):
        await create_country()

    SQL = 'SELECT COUNT(*) AS among FROM public.country'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_state(conn, create_state):
    EXPECTED_COUNT = 1
    await create_state()

    SQL_STATE = 'SELECT COUNT(*) AS among FROM public.state'
    result_state = await conn.select(SQL_STATE, None, True)
    assert result_state['among'] == EXPECTED_COUNT

    SQL_COUNTRY = 'SELECT COUNT(*) AS among FROM public.country'
    result_country = await conn.select(SQL_COUNTRY, None, True)
    assert result_country['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_city(conn, create_city):
    EXPECTED_COUNT = 1
    city_data = await create_city()

    assert city_data.get('state_id') is not None
    assert city_data.get('country_id') is not None

    SQL = 'SELECT COUNT(*) AS among FROM public.city'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_institution(conn, create_institution):
    EXPECTED_COUNT = 1
    specific_name = 'Instituto de Testes Avançados'

    institution_data = await create_institution(name=specific_name)

    assert institution_data['name'] == specific_name

    SQL = 'SELECT COUNT(*) AS among FROM public.institution'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_researcher(conn, create_researcher):
    EXPECTED_COUNT = 1

    researcher_data = await create_researcher(name='Ada Lovelace')

    assert researcher_data['name'] == 'Ada Lovelace'

    assert researcher_data.get('city_id') is not None
    assert researcher_data.get('institution_id') is not None

    SQL = 'SELECT COUNT(*) AS among FROM public.researcher'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_event_with_automatic_researcher(
    create_participation_event,
):
    event_data = await create_participation_event(title='Evento Automático')

    assert event_data['title'] == 'Evento Automático'
    assert event_data.get('researcher_id') is not None


@pytest.mark.asyncio
async def test_create_event_for_specific_researcher(
    create_researcher, create_participation_event
):
    pesquisadora_alvo = await create_researcher(name='Marie Curie')
    pesquisadora_id = pesquisadora_alvo['id']

    event_data = await create_participation_event(
        title='Estudos sobre a radioatividade', researcher_id=pesquisadora_id
    )

    assert event_data['title'] == 'Estudos sobre a radioatividade'
    assert event_data['researcher_id'] == pesquisadora_id


@pytest.mark.asyncio
async def test_create_program_with_auto_institution(
    conn, create_graduate_program
):
    EXPECTED_COUNT = 1

    program_data = await create_graduate_program(
        name='Programa de Pós-Graduação em IA'
    )

    assert program_data['name'] == 'Programa de Pós-Graduação em IA'
    assert program_data.get('institution_id') is not None

    SQL = 'SELECT COUNT(*) AS among FROM public.graduate_program'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_program_for_specific_institution(
    create_institution, create_graduate_program
):
    instituicao_alvo = await create_institution(
        name='Universidade Federal da Bahia'
    )
    instituicao_id = instituicao_alvo['id']

    program_data = await create_graduate_program(
        name='Mestrado em Ciência da Computação', institution_id=instituicao_id
    )

    assert program_data['name'] == 'Mestrado em Ciência da Computação'
    assert program_data['institution_id'] == instituicao_id


@pytest.mark.asyncio
async def test_link_researcher_program_automatically(
    conn, link_researcher_to_program
):
    EXPECTED_COUNT = 1

    link_info = await link_researcher_to_program()

    assert link_info.get('researcher_id') is not None
    assert link_info.get('graduate_program_id') is not None

    SQL = 'SELECT COUNT(*) AS among FROM public.graduate_program_researcher'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_link_specific_researcher_to_program(
    conn, create_researcher, create_graduate_program, link_researcher_to_program
):
    pesquisador = await create_researcher(name='Isaac Newton')
    programa = await create_graduate_program(name='Mestrado em Física Clássica')

    await link_researcher_to_program(
        researcher_id=pesquisador['id'],
        graduate_program_id=programa['graduate_program_id'],
        type_='COLABORADOR',
    )

    SQL = """
        SELECT type_ FROM public.graduate_program_researcher
        WHERE researcher_id = %(researcher_id)s AND graduate_program_id = %(graduate_program_id)s
    """
    params = {
        'researcher_id': pesquisador['id'],
        'graduate_program_id': programa['graduate_program_id'],
    }
    result = await conn.select(SQL, params, True)

    assert result is not None
    assert result['type_'] == 'COLABORADOR'


@pytest.mark.asyncio
async def test_create_department(conn, create_department):
    EXPECTED_COUNT = 1

    await create_department(dep_nom='Departamento de Ciência da Computação')

    SQL = 'SELECT COUNT(*) AS among FROM ufmg.departament'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_link_department_researcher_automatically(
    conn, link_researcher_to_department
):
    EXPECTED_COUNT = 1

    link_info = await link_researcher_to_department()

    assert link_info.get('researcher_id') is not None
    assert link_info.get('dep_id') is not None

    SQL = 'SELECT COUNT(*) AS among FROM ufmg.departament_researcher'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_link_specific_researcher_to_department(
    conn, create_researcher, create_department, link_researcher_to_department
):
    pesquisador = await create_researcher(name='Alan Turing')
    departamento = await create_department(dep_nom='Inteligência Artificial')

    await link_researcher_to_department(
        researcher_id=pesquisador['id'], dep_id=departamento['dep_id']
    )

    SQL = """
        SELECT researcher_id FROM ufmg.departament_researcher
        WHERE dep_id = %(dep_id)s
        """
    result = await conn.select(SQL, {'dep_id': departamento['dep_id']}, True)

    assert result is not None
    assert str(result['researcher_id']) == pesquisador['id']


# Cenário 1: Criar o fomento com um pesquisador automático
@pytest.mark.asyncio
async def test_create_foment_automatically(conn, create_foment):
    """
    Testa a criação de um registro de fomento sem especificar o pesquisador.
    """
    EXPECTED_COUNT = 1

    # A fixture `create_researcher` será chamada em background
    foment_info = await create_foment(call_title='Edital Universal CNPq')

    assert foment_info['call_title'] == 'Edital Universal CNPq'
    assert foment_info.get('researcher_id') is not None

    # Confirma que o registro foi inserido no banco
    SQL = 'SELECT COUNT(*) AS among FROM public.foment'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_foment_for_specific_researcher(
    create_researcher, create_foment
):
    pesquisador_alvo = await create_researcher(name='Albert Einstein')

    foment_info = await create_foment(
        researcher_id=pesquisador_alvo['id'],
        modality_name='Bolsa de Produtividade em Pesquisa',
    )

    assert foment_info['researcher_id'] == pesquisador_alvo['id']
    assert foment_info['modality_name'] == 'Bolsa de Produtividade em Pesquisa'


@pytest.mark.asyncio
async def test_create_researcher_production_auto(
    conn, create_researcher_production
):
    EXPECTED_COUNT = 1

    production_info = await create_researcher_production(articles=10)

    assert production_info['articles'] == 10
    assert production_info.get('researcher_id') is not None

    SQL = 'SELECT COUNT(*) AS among FROM public.researcher_production'
    result = await conn.select(SQL, None, True)
    assert result['among'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_production_for_specific_researcher(
    conn, create_researcher, create_researcher_production
):
    pesquisador_alvo = await create_researcher(name='Galileu Galilei')

    production_info = await create_researcher_production(
        researcher_id=pesquisador_alvo['id'], book=2, articles=5
    )

    assert production_info['researcher_id'] == pesquisador_alvo['id']
    assert production_info['book'] == 2

    SQL = """
        SELECT articles FROM public.researcher_production
        WHERE researcher_id = %(researcher_id)s
    """
    result = await conn.select(
        SQL, {'researcher_id': pesquisador_alvo['id']}, True
    )
    assert result['articles'] == 5


@pytest.mark.asyncio
async def test_create_research_group(conn, create_research_group):
    EXPECTED_COUNT = 1
    test_name = 'Grupo de Pesquisa em Sistemas Inteligentes'

    group_data = await create_research_group(name=test_name)

    assert group_data['name'] == test_name
    assert group_data.get('id') is not None

    SQL = 'SELECT COUNT(*) AS total FROM public.research_group'
    result = await conn.select(SQL, None, True)
    assert result['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_link_researcher_to_group_with_auto_creation(
    conn, link_researcher_to_research_group
):
    EXPECTED_COUNT = 1

    link_data = await link_researcher_to_research_group()

    assert link_data.get('researcher_id') is not None
    assert link_data.get('research_group_id') is not None

    SQL = 'SELECT COUNT(*) AS total FROM public.research_group_researcher'
    result = await conn.select(SQL, None, True)
    assert result['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_link_researcher_to_specific_group(
    conn,
    create_researcher,
    create_research_group,
    link_researcher_to_research_group,
):
    pesquisador_alvo = await create_researcher(name='Ada Lovelace')
    grupo_alvo = await create_research_group(
        name='Grupo de Estudos de Motores Analíticos'
    )

    pesquisador_id = pesquisador_alvo['id']
    grupo_id = grupo_alvo['id']

    link_data = await link_researcher_to_research_group(
        researcher_id=pesquisador_id, research_group_id=grupo_id
    )

    assert link_data['researcher_id'] == pesquisador_id
    assert link_data['research_group_id'] == grupo_id

    SQL = """
        SELECT * FROM public.research_group_researcher
        WHERE researcher_id = %(researcher_id)s AND research_group_id = %(group_id)s
    """
    params = {'researcher_id': pesquisador_id, 'group_id': grupo_id}
    result = await conn.select(SQL, params, False)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_create_periodical_magazine(conn, create_periodical_magazine):
    EXPECTED_COUNT = 1

    await create_periodical_magazine()

    SQL = 'SELECT COUNT(*) AS total FROM public.periodical_magazine'

    result = await conn.select(SQL, None, True)

    assert result['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_bibliographic_production_article(
    conn, create_bibliographic_production_article
):
    EXPECTED_COUNT = 1

    await create_bibliographic_production_article()

    SQL = 'SELECT COUNT(*) AS total FROM public.bibliographic_production_article'
    result_article = await conn.select(SQL, None, True)
    assert result_article['total'] == EXPECTED_COUNT

    SQL_PRODUCTION = (
        'SELECT COUNT(*) AS total FROM public.bibliographic_production'
    )
    result_production = await conn.select(SQL_PRODUCTION, None, True)
    assert result_production['total'] == EXPECTED_COUNT

    SQL_MAGAZINE = 'SELECT COUNT(*) AS total FROM public.periodical_magazine'
    result_magazine = await conn.select(SQL_MAGAZINE, None, True)
    assert result_magazine['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_patent(conn, create_patent):
    EXPECTED_COUNT = 1

    await create_patent()

    SQL_PATENT = 'SELECT COUNT(*) AS total FROM public.patent'
    result_patent = await conn.select(SQL_PATENT, None, True)
    assert result_patent['total'] == EXPECTED_COUNT

    SQL_RESEARCHER = 'SELECT COUNT(*) AS total FROM public.researcher'
    result_researcher = await conn.select(SQL_RESEARCHER, None, True)
    assert result_researcher['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_brand(conn, create_brand):
    EXPECTED_COUNT = 1

    await create_brand()

    SQL_BRAND = 'SELECT COUNT(*) AS total FROM public.brand'
    result_brand = await conn.select(SQL_BRAND, None, True)
    assert result_brand['total'] == EXPECTED_COUNT

    SQL_RESEARCHER = 'SELECT COUNT(*) AS total FROM public.researcher'
    result_researcher = await conn.select(SQL_RESEARCHER, None, True)
    assert result_researcher['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_researcher_professional_experience(
    conn, create_researcher_professional_experience
):
    EXPECTED_COUNT = 1

    await create_researcher_professional_experience()

    SQL_EXPERIENCE = (
        'SELECT COUNT(*) AS total FROM public.researcher_professional_experience'
    )
    result_experience = await conn.select(SQL_EXPERIENCE, None, True)
    assert result_experience['total'] == EXPECTED_COUNT

    SQL_RESEARCHER = 'SELECT COUNT(*) AS total FROM public.researcher'
    result_researcher = await conn.select(SQL_RESEARCHER, None, True)
    assert result_researcher['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_research_report(conn, create_research_report):
    EXPECTED_COUNT = 1

    await create_research_report()

    SQL_REPORT = 'SELECT COUNT(*) AS total FROM public.research_report'
    result_report = await conn.select(SQL_REPORT, None, True)
    assert result_report['total'] == EXPECTED_COUNT

    SQL_RESEARCHER = 'SELECT COUNT(*) AS total FROM public.researcher'
    result_researcher = await conn.select(SQL_RESEARCHER, None, True)
    assert result_researcher['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_bibliographic_production_book_chapter(
    conn, create_bibliographic_production_book_chapter
):
    EXPECTED_COUNT = 1

    await create_bibliographic_production_book_chapter()

    SQL = 'SELECT COUNT(*) AS total FROM public.bibliographic_production_book_chapter'
    result_book_chapter = await conn.select(SQL, None, True)
    assert result_book_chapter['total'] == EXPECTED_COUNT

    SQL_PRODUCTION = (
        'SELECT COUNT(*) AS total FROM public.bibliographic_production'
    )
    result_production = await conn.select(SQL_PRODUCTION, None, True)
    assert result_production['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_software(conn, create_software):
    EXPECTED_COUNT = 1

    await create_software()

    SQL = 'SELECT COUNT(*) AS total FROM public.software'
    result_software = await conn.select(SQL, None, True)
    assert result_software['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_guidance(conn, create_guidance):
    EXPECTED_COUNT = 1

    await create_guidance()

    SQL = 'SELECT COUNT(*) AS total FROM public.guidance'
    result_guidance = await conn.select(SQL, None, True)
    assert result_guidance['total'] == EXPECTED_COUNT


# Em seu arquivo de testes


@pytest.mark.asyncio
async def test_create_bibliographic_production_work_in_event(
    conn, create_bibliographic_production_work_in_event
):
    EXPECTED_COUNT = 1

    await create_bibliographic_production_work_in_event()

    SQL = 'SELECT COUNT(*) AS total FROM public.bibliographic_production_work_in_event'
    result = await conn.select(SQL, None, True)

    assert result['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_research_project(conn, create_research_project):
    EXPECTED_COUNT = 1

    await create_research_project()

    SQL = 'SELECT COUNT(*) AS total FROM public.research_project'
    result = await conn.select(SQL, None, True)

    assert result['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_institution_admin(conn_admin, create_institution_admin):
    EXPECTED_COUNT = 1

    await create_institution_admin()

    SQL = 'SELECT COUNT(*) AS total FROM public.institution'

    result = await conn_admin.select(SQL, None, True)

    assert result['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_collection(conn_admin, create_collection):
    EXPECTED_COUNT = 1

    await create_collection()

    SQL = 'SELECT COUNT(*) AS total FROM feature.collection'
    result = await conn_admin.select(SQL, None, True)

    assert result['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_collection_entry(conn_admin, create_collection_entry):
    EXPECTED_COUNT = 1

    await create_collection_entry()

    SQL = 'SELECT COUNT(*) AS total FROM feature.collection_entries'
    result = await conn_admin.select(SQL, None, True)

    assert result['total'] == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_create_star_entry(conn_admin, create_star_entry):
    EXPECTED_COUNT = 1

    await create_star_entry()

    SQL = 'SELECT COUNT(*) AS total FROM feature.stars'
    result = await conn_admin.select(SQL, None, True)

    assert result['total'] == EXPECTED_COUNT
