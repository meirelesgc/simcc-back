from http import HTTPStatus

import pytest

ENDPOINT_URL = '/production/events/participation'


@pytest.mark.asyncio
async def test_get_all_pevents(client, create_participation_event):
    await create_participation_event()
    await create_participation_event()
    expected_count = 2

    response = client.get(ENDPOINT_URL)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(client, create_participation_event):
    target_title = 'Apresentação sobre Inteligência Artificial'
    await create_participation_event(title='Outro Evento Aleatório')
    await create_participation_event(title=target_title)

    expected_count = 1
    params = {'term': 'Inteligência Artificial'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['title'] == target_title


@pytest.mark.asyncio
async def test_filter_by_year(client, create_participation_event):
    await create_participation_event(year='2021')
    await create_participation_event(year='2024')

    expected_count = 1
    params = {'year': '2023'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_name(
    client,
    create_participation_event,
    create_department,
    link_researcher_to_department,
):
    department = await create_department(dep_nom='Ciência da Computação')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_participation_event()
    await create_participation_event(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'departament': 'Ciência da Computação'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client,
    create_participation_event,
    create_department,
    link_researcher_to_department,
):
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_participation_event()
    await create_participation_event(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'dep_id': department['dep_id']}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(client, create_participation_event):
    await create_participation_event()
    pevent_to_find = await create_participation_event()

    expected_count = 1
    params = {'researcher_id': pevent_to_find['researcher_id']}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client, create_participation_event, create_institution, create_researcher
):
    institution = await create_institution(
        name='Instituto Tecnológico de Aeronáutica'
    )
    researcher = await create_researcher(institution_id=institution['id'])

    await create_participation_event()
    await create_participation_event(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution': 'Instituto Tecnológico de Aeronáutica'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_name(
    client,
    create_participation_event,
    create_graduate_program,
    link_researcher_to_program,
):
    program = await create_graduate_program(name='Engenharia de Software')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_participation_event()
    await create_participation_event(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'graduate_program': 'Engenharia de Software'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client,
    create_participation_event,
    create_graduate_program,
    link_researcher_to_program,
):
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_participation_event()
    await create_participation_event(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'graduate_program_id': program['graduate_program_id']}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_city(
    client, create_participation_event, create_researcher_production
):
    researcher_prod = await create_researcher_production(city='São Paulo')

    await create_participation_event()
    await create_participation_event(
        researcher_id=researcher_prod['researcher_id']
    )

    expected_count = 1
    params = {'city': 'São Paulo'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    conn, client, create_participation_event, create_researcher_production
):
    areas = ['Ciências Sociais Aplicadas', 'Engenharias']
    researcher_prod = await create_researcher_production(great_area_=areas)

    await create_participation_event()
    await create_participation_event(
        researcher_id=researcher_prod['researcher_id']
    )
    expected_count = 1
    params = {'area': 'Engenharias'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(
    client, create_participation_event, create_foment
):
    foment = await create_foment(modality_name='Bolsa de Iniciação Científica')

    await create_participation_event()
    await create_participation_event(researcher_id=foment['researcher_id'])

    expected_count = 1
    params = {'modality': 'Bolsa de Iniciação Científica'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(
    client, create_participation_event, create_researcher
):
    researcher = await create_researcher(graduation='Mestrado')

    await create_participation_event()
    await create_participation_event(researcher_id=researcher['id'])

    expected_count = 1
    params = {'graduation': 'Mestrado'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_title(
    client, create_participation_event, create_researcher
):
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Evento com Título Repetido'
    await create_participation_event(
        title=common_title, researcher_id=researcher1['id']
    )
    await create_participation_event(
        title=common_title, researcher_id=researcher2['id']
    )
    await create_participation_event(title='Evento de Título Único')

    expected_count = 2
    params = {'distinct': 'true'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_participation_event):
    for i in range(7):
        await create_participation_event(title=f'Evento {i}')

    expected_count = 3
    params = {'page': '2', 'lenght': '3'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_lattes_id(
    client, create_participation_event, create_researcher
):
    target_lattes_id = '9876543210987654'
    researcher = await create_researcher(lattes_id=target_lattes_id)

    await create_participation_event()
    await create_participation_event(researcher_id=researcher['id'])

    expected_count = 1
    params = {'lattes_id': target_lattes_id}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_group_id(
    client,
    create_participation_event,
    create_research_group,
    link_researcher_to_research_group,
):
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )

    await create_participation_event()
    await create_participation_event(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'group_id': group['id']}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_nature(client, create_participation_event):
    target_nature = 'Natureza morta'
    await create_participation_event(nature='Outra Natureza Aleatório')
    await create_participation_event(nature=target_nature)

    expected_count = 1
    params = {'nature': target_nature}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_id(
    client, create_participation_event, create_institution, create_researcher
):
    """Testa o filtro por ID da instituição do pesquisador."""
    # Arrange
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    await create_participation_event()
    await create_participation_event(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution_id': institution['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_group_name(
    client,
    create_participation_event,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome do grupo de pesquisa."""
    # Arrange
    group = await create_research_group(name='Observatório de Mídia')
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )

    await create_participation_event()
    await create_participation_event(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'group': 'Observatório de Mídia'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_collection_id(
    client, create_participation_event, create_collection_entry
):
    """Testa o filtro por ID de uma coleção."""
    # Arrange
    await create_participation_event()
    event_in_collection = await create_participation_event()

    collection = await create_collection_entry(
        entry_id=event_in_collection['id'], type='EVENT_PARTICIPATION'
    )

    expected_count = 1
    params = {'collection_id': collection['collection_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
