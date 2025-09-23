from http import HTTPStatus

import pytest

from simcc.schemas import Researcher

ENDPOINT_URL = '/researcher/foment'


@pytest.mark.asyncio
async def test_get_researcher_foment(conn, client, create_foment):
    # Arrange
    expected_count = 2
    await create_foment()
    await create_foment()

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert [Researcher.Researcher(**item) for item in data]


@pytest.mark.asyncio
async def test_term_filter_on_foment_title(client, create_foment):
    # Arrange
    target_title = 'Edital Universal de Fomento à Pesquisa'
    await create_foment(abstract='Chamada Pública para Inovação')
    await create_foment(abstract=target_title)

    expected_count = 1
    params = {'term': 'Fomento'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduate_program_filter(
    client,
    create_foment,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    graduate_program = await create_graduate_program()
    researcher_linked = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_foment()
    await create_foment(researcher_id=researcher_linked['researcher_id'])

    expected_count = 1
    params = {'graduate_program': graduate_program['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_department_filter(
    client,
    create_foment,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por nome do departamento."""
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_foment()
    await create_foment(researcher_id=link_info['researcher_id'])

    expected_count = 1
    params = {'departament': department['dep_nom']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_institution_filter(
    client,
    create_foment,
    create_institution,
    create_researcher,
):
    """Testa o filtro por nome da instituição."""
    # Arrange
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    await create_foment()
    await create_foment(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution': institution['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_city_filter(client, create_foment, create_researcher_production):
    # Arrange
    city = 'Campinas'
    await create_foment()
    researcher = await create_foment()
    await create_researcher_production(
        researcher_id=researcher['researcher_id'], city=city
    )

    expected_count = 1
    params = {'city': city}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_area_filter(client, create_foment, create_researcher_production):
    # Arrange
    great_area = ['Ciências Exatas e da Terra', 'Engenharias']
    await create_foment()
    researcher = await create_foment()
    await create_researcher_production(
        researcher_id=researcher['researcher_id'], great_area_=great_area
    )
    expected_count = 1
    params = {'area': ';'.join(great_area)}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_research_group_filter_by_name(
    client,
    create_foment,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome do grupo de pesquisa."""
    # Arrange
    research_group = await create_research_group()
    linked_researcher = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    await create_foment()
    await create_foment(researcher_id=linked_researcher['researcher_id'])

    expected_count = 1
    params = {'group': research_group['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_modality_filter(client, create_foment):
    # Arrange
    target_modality = 'Bolsa de Produtividade em Pesquisa'
    await create_foment(modality_name='Auxílio à Pesquisa')
    await create_foment(modality_name=target_modality)
    expected_count = 1
    params = {'modality': target_modality}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_researcher_id_filter(client, create_foment):
    """Testa o filtro por ID do pesquisador."""
    # Arrange
    foment1 = await create_foment()
    foment2 = await create_foment()

    expected_count = 1
    params = {'researcher_id': foment2['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == foment2['researcher_id']


@pytest.mark.asyncio
async def test_lattes_id_filter(client, create_foment):
    """Testa o filtro por ID Lattes do pesquisador."""
    # Arrange
    lattes_id = '1234567890123456'
    await create_foment()
    await create_foment(lattes_id=lattes_id)

    expected_count = 1
    params = {'lattes_id': lattes_id}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduation_filter(client, create_foment):
    """Testa o filtro por nível de graduação do pesquisador."""
    # Arrange
    graduation = 'Doutorado'
    await create_foment(graduation='Mestrado')
    await create_foment(graduation=graduation)

    expected_count = 1
    params = {'graduation': graduation}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_department_id_filter(
    client,
    create_foment,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por ID do departamento."""
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_foment()
    await create_foment(researcher_id=link_info['researcher_id'])

    expected_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduate_program_id_filter(
    client,
    create_foment,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por ID do programa de pós-graduação."""
    # Arrange
    graduate_program = await create_graduate_program()
    researcher = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_foment()
    await create_foment(researcher_id=researcher['researcher_id'])

    expected_count = 1
    params = {'graduate_program_id': graduate_program['graduate_program_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_research_group_filter_by_id(
    client,
    create_foment,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por ID do grupo de pesquisa."""
    # Arrange
    research_group = await create_research_group()

    linked_researcher = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    await create_foment()
    await create_foment(researcher_id=linked_researcher['researcher_id'])

    expected_count = 1
    params = {'group_id': research_group['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_star(
    client,
    create_foment,
    create_star_entry,
    override_get_current_user,
):
    await create_foment()
    researcher = await create_foment()

    star = await create_star_entry(
        entry_id=researcher['researcher_id'],
        type='RESEARCHER',
    )

    override_get_current_user({'user_id': star['user_id']})

    expected_count = 1

    params = {'star': True, 'type': 'ARTICLE'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
