from http import HTTPStatus

import pytest

ENDPOINT_URL = '/software_production_researcher'


@pytest.mark.asyncio
async def test_get_all_software(client, create_software):
    # Arrange
    await create_software()
    await create_software()
    expected_count = 2

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(client, create_software):
    # Arrange
    target_title = 'Software de Análise Preditiva'
    await create_software(title='Sistema de Gestão Antigo')
    await create_software(title=target_title)
    expected_count = 1
    params = {'term': 'Preditiva'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_year(client, create_software):
    # Arrange
    await create_software(year=2021)
    await create_software(year=2023)
    expected_count = 1
    params = {'year': 2022}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_name(
    client,
    create_software,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department(dep_nom='Ciência da Computação')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_software()
    await create_software(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'departament': 'Ciência da Computação'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client,
    create_software,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_software()
    await create_software(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(
    client, create_software, create_researcher
):
    # Arrange
    researcher = await create_researcher()
    await create_software()
    await create_software(researcher_id=researcher['id'])
    expected_count = 1
    params = {'researcher_id': researcher['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client,
    create_software,
    create_institution,
    create_researcher,
):
    # Arrange
    institution = await create_institution(
        name='Universidade Federal de Tecnologia'
    )
    researcher = await create_researcher(institution_id=institution['id'])
    await create_software()
    await create_software(researcher_id=researcher['id'])
    expected_count = 1
    params = {'institution': 'Universidade Federal de Tecnologia'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_name(
    client,
    create_software,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program(name='Engenharia de Software')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_software()
    await create_software(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'graduate_program': 'Engenharia de Software'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client,
    create_software,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_software()
    await create_software(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'graduate_program_id': program['graduate_program_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_city(
    client, create_software, create_researcher_production
):
    # Arrange
    researcher_prod = await create_researcher_production(city='Belo Horizonte')
    await create_software()
    await create_software(researcher_id=researcher_prod['researcher_id'])
    expected_count = 1
    params = {'city': 'Belo Horizonte'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    client, create_software, create_researcher_production
):
    # Arrange
    areas = ['Ciências Exatas e da Terra', 'Engenharias']
    researcher_prod = await create_researcher_production(great_area_=areas)
    await create_software()
    await create_software(researcher_id=researcher_prod['researcher_id'])
    expected_count = 1
    params = {'area': 'Engenharias'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(client, create_software, create_foment):
    # Arrange
    foment = await create_foment(modality_name='Bolsa de Iniciação Tecnológica')
    await create_software()
    await create_software(researcher_id=foment['researcher_id'])
    expected_count = 1
    params = {'modality': 'Bolsa de Iniciação Tecnológica'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(client, create_software, create_researcher):
    # Arrange
    researcher = await create_researcher(graduation='Doutorado')
    await create_software()
    await create_software(researcher_id=researcher['id'])
    expected_count = 1
    params = {'graduation': 'Doutorado'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_title(
    client, create_software, create_researcher
):
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Software com Título Repetido'
    await create_software(title=common_title, researcher_id=researcher1['id'])
    await create_software(title=common_title, researcher_id=researcher2['id'])
    await create_software(title='Software de Título Único')
    expected_count = 2
    params = {'distinct': 'true'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_software):
    # Arrange
    for i in range(5):
        await create_software(title=f'Software {i}')
    expected_count = 2
    params = {'page': '2', 'lenght': '2'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_lattes_id(client, create_software, create_researcher):
    # Arrange
    target_lattes_id = '1122334455667788'
    researcher = await create_researcher(lattes_id=target_lattes_id)
    await create_software()
    await create_software(researcher_id=researcher['id'])
    expected_count = 1
    params = {'lattes_id': target_lattes_id}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_group_id(
    client,
    create_software,
    create_research_group,
    link_researcher_to_research_group,
):
    # Arrange
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_software()
    await create_software(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'group_id': group['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
