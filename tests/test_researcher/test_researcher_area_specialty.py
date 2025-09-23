from http import HTTPStatus

import pytest

from simcc.schemas import Researcher

ENDPOINT_URL = '/researcherArea_specialty'


@pytest.mark.asyncio
async def test_get_researcher_area_specialty(
    client, create_researcher_production
):
    # Arrange
    expected_count = 2
    for _ in range(expected_count):
        await create_researcher_production(area_specialty='Ciência de Dados')

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert [Researcher.Researcher(**item) for item in data]


@pytest.mark.asyncio
async def test_term_filter(client, create_researcher_production):
    # Arrange
    target_specialty = 'Inteligência Artificial'
    await create_researcher_production(area_specialty='Computação Gráfica')
    await create_researcher_production(area_specialty=target_specialty)

    expected_count = 1
    params = {'term': target_specialty}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] is not None


@pytest.mark.asyncio
async def test_graduate_program_filter(
    client,
    create_researcher_production,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    graduate_program = await create_graduate_program()
    researcher_linked = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_researcher_production()
    await create_researcher_production(
        researcher_id=researcher_linked['researcher_id']
    )

    expected_count = 1
    params = {'graduate_program': graduate_program['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduate_program_id_filter(
    client,
    create_researcher_production,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    graduate_program = await create_graduate_program()
    researcher_linked = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_researcher_production()
    await create_researcher_production(
        researcher_id=researcher_linked['researcher_id']
    )

    expected_count = 1
    params = {'graduate_program_id': graduate_program['graduate_program_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_researcher_id_filter(client, create_researcher_production):
    # Arrange
    await create_researcher_production()
    target_production = await create_researcher_production()

    expected_count = 1
    params = {'researcher_id': target_production['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_department_filter(
    client,
    create_researcher_production,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_researcher_production()
    await create_researcher_production(researcher_id=link_info['researcher_id'])

    expected_count = 1
    params = {'departament': department['dep_nom']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_department_id_filter(
    client,
    create_researcher_production,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_researcher_production()
    await create_researcher_production(researcher_id=link_info['researcher_id'])

    expected_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_institution_filter(
    client, create_researcher_production, create_institution, create_researcher
):
    # Arrange
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    await create_researcher_production()
    await create_researcher_production(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution': institution['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_city_filter(client, create_researcher_production):
    # Arrange
    target_city = 'Belo Horizonte'
    await create_researcher_production(city='Qualquer Outra')
    await create_researcher_production(city=target_city)

    expected_count = 1
    params = {'city': target_city}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_area_filter(client, create_researcher_production):
    # Arrange
    target_area = ['CIENCIAS_EXATAS_E_DA_TERRA', 'ENGENHARIAS']
    await create_researcher_production(great_area_=['CIENCIAS_BIOLOGICAS'])
    await create_researcher_production(great_area_=target_area)

    expected_count = 1
    params = {'area': ';'.join(target_area)}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_modality_filter(
    client, create_researcher_production, create_foment
):
    """
    Testa o filtro por modalidade de fomento.
    """
    # Arrange
    foment_info = await create_foment(
        modality_name='Bolsa de Produtividade em Pesquisa'
    )
    await create_researcher_production()
    await create_researcher_production(
        researcher_id=foment_info['researcher_id']
    )

    expected_count = 1
    params = {'modality': foment_info['modality_name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduation_filter(
    client, create_researcher_production, create_researcher
):
    """
    Testa o filtro por nível de graduação.
    """
    # Arrange
    target_graduation = 'Doutorado'
    researcher_with_grad = await create_researcher(graduation=target_graduation)

    await create_researcher_production()
    await create_researcher_production(researcher_id=researcher_with_grad['id'])

    expected_count = 1
    params = {'graduation': target_graduation}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_page_filter(client, create_researcher_production):
    """
    Testa a paginação (limit e offset) dos resultados.
    """
    # Arrange
    total_items = 5
    page_size = 3
    for _ in range(total_items):
        await create_researcher_production()

    params = {'page': 1, 'lenght': page_size}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == page_size


@pytest.mark.asyncio
async def test_lattes_id_filter(
    client, create_researcher_production, create_researcher
):
    """
    Testa o filtro por ID Lattes do pesquisador.
    """
    # Arrange
    target_lattes_id = '1234567890123456'
    researcher_with_lattes = await create_researcher(lattes_id=target_lattes_id)

    await create_researcher_production()
    await create_researcher_production(
        researcher_id=researcher_with_lattes['id']
    )

    expected_count = 1
    params = {'lattes_id': target_lattes_id}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_research_group_filter_by_id(
    client,
    create_researcher_production,
    create_research_group,
    link_researcher_to_research_group,
):
    """
    Testa o filtro por ID do grupo de pesquisa.
    """
    # Arrange
    research_group = await create_research_group()
    linked_researcher = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    await create_researcher_production()
    await create_researcher_production(
        researcher_id=linked_researcher['researcher_id']
    )

    expected_count = 1
    params = {'group_id': research_group['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_research_group_filter_by_name(
    client,
    create_researcher_production,
    create_research_group,
    link_researcher_to_research_group,
):
    # Arrange
    research_group = await create_research_group()
    linked_researcher = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    await create_researcher_production()
    await create_researcher_production(
        researcher_id=linked_researcher['researcher_id']
    )

    expected_count = 1
    params = {'group': research_group['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_star(
    client,
    create_researcher_production,
    create_star_entry,
    override_get_current_user,
):
    expected_count = 2
    for _ in range(expected_count):
        researcher = await create_researcher_production(
            area_specialty='Ciência de Dados'
        )

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
