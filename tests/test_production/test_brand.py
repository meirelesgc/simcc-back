# tests/test_brand_production_researcher.py

from http import HTTPStatus

import pytest

ENDPOINT_URL = '/brand_production_researcher'


@pytest.mark.asyncio
async def test_get_all_brands(client, create_brand):
    # Arrange
    await create_brand()
    await create_brand()
    expected_count = 2

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(client, create_brand):
    # Arrange
    target_title = 'Marca Inovadora Global'
    await create_brand(title='Outra Marca Aleatória')
    await create_brand(title=target_title)
    expected_count = 1
    params = {'term': 'Inovadora'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['title'] == target_title


@pytest.mark.asyncio
async def test_filter_by_year(client, create_brand):
    # Arrange
    await create_brand(year=2021)
    await create_brand(year=2024)
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
    create_brand,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department(dep_nom='Design Gráfico')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_brand()
    await create_brand(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'departament': 'Design Gráfico'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client,
    create_brand,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_brand()
    await create_brand(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(client, create_brand):
    # Arrange
    await create_brand()
    brand_to_find = await create_brand()
    expected_count = 1
    params = {'researcher_id': brand_to_find['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client,
    create_brand,
    create_institution,
    create_researcher,
):
    # Arrange
    institution = await create_institution(
        name='Escola Superior de Propaganda e Marketing'
    )
    researcher = await create_researcher(institution_id=institution['id'])
    await create_brand()
    await create_brand(researcher_id=researcher['id'])
    expected_count = 1
    params = {'institution': 'Escola Superior de Propaganda e Marketing'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_name(
    client,
    create_brand,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program(
        name='Comunicação e Práticas de Consumo'
    )
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_brand()
    await create_brand(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'graduate_program': 'Comunicação e Práticas de Consumo'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client,
    create_brand,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_brand()
    await create_brand(researcher_id=linked['researcher_id'])
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
    client, create_brand, create_researcher_production
):
    # Arrange
    researcher_prod = await create_researcher_production(city='São Paulo')
    await create_brand()
    await create_brand(researcher_id=researcher_prod['researcher_id'])
    expected_count = 1
    params = {'city': 'São Paulo'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    client, create_brand, create_researcher_production
):
    # Arrange
    areas = ['Ciências Sociais Aplicadas', 'Linguística']
    researcher_prod = await create_researcher_production(great_area_=areas)
    await create_brand()
    await create_brand(researcher_id=researcher_prod['researcher_id'])
    expected_count = 1
    params = {'area': 'Linguística'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(client, create_brand, create_foment):
    # Arrange
    foment = await create_foment(modality_name='Inovação em Pequenas Empresas')
    await create_brand()
    await create_brand(researcher_id=foment['researcher_id'])
    expected_count = 1
    params = {'modality': 'Inovação em Pequenas Empresas'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(client, create_brand, create_researcher):
    # Arrange
    researcher = await create_researcher(graduation='Doutorado')
    await create_brand()
    await create_brand(researcher_id=researcher['id'])
    expected_count = 1
    params = {'graduation': 'Doutorado'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_title(client, create_brand, create_researcher):
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Marca com Título Repetido'
    await create_brand(title=common_title, researcher_id=researcher1['id'])
    await create_brand(title=common_title, researcher_id=researcher2['id'])
    await create_brand(title='Marca de Título Único')
    expected_count = 2
    params = {'distinct': 'true'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_brand):
    # Arrange
    for i in range(5):
        await create_brand(title=f'Marca {i}')
    expected_count = 2
    params = {'page': '2', 'lenght': '2'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_lattes_id(client, create_brand, create_researcher):
    # Arrange
    target_lattes_id = '1122334455667788'
    researcher = await create_researcher(lattes_id=target_lattes_id)
    await create_brand()
    await create_brand(researcher_id=researcher['id'])
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
    create_brand,
    create_research_group,
    link_researcher_to_research_group,
):
    # Arrange
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_brand()
    await create_brand(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'group_id': group['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
