# tests/test_patent_production.py

from http import HTTPStatus

import pytest

ENDPOINT_URL = '/patent_production_researcher'


@pytest.mark.asyncio
async def test_get_all_patents(client, create_patent):
    await create_patent()
    await create_patent()
    expected_count = 2

    response = client.get(ENDPOINT_URL)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(client, create_patent):
    """
    Testa o filtro por termo no título da patente.
    """
    # Arrange
    target_title = 'Dispositivo Inovador para Energia Limpa'
    await create_patent(title='Outra Patente Aleatória')
    await create_patent(title=target_title)

    expected_count = 1
    params = {'term': 'Energia Limpa'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['title'] == target_title


@pytest.mark.asyncio
async def test_filter_by_year(client, create_patent):
    # Arrange
    await create_patent(development_year='2020')
    await create_patent(development_year='2023')

    expected_count = 1
    params = {
        'year': '2022'
    }  # Deve retornar apenas patentes de 2022 ou mais recentes

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_name(
    client, create_patent, create_department, link_researcher_to_department
):
    # Arrange
    department = await create_department(dep_nom='Engenharia Elétrica')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_patent()  # Patente não relacionada
    await create_patent(
        researcher_id=linked['researcher_id']
    )  # Patente relacionada

    expected_count = 1
    params = {'departament': 'Engenharia Elétrica'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client, create_patent, create_department, link_researcher_to_department
):
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(client, create_patent):
    # Arrange
    await create_patent()
    patent_to_find = await create_patent()

    expected_count = 1
    params = {'researcher_id': patent_to_find['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client, create_patent, create_institution, create_researcher
):
    # Arrange
    institution = await create_institution(
        name='Universidade Federal de Minas Gerais'
    )
    researcher = await create_researcher(institution_id=institution['id'])

    await create_patent()
    await create_patent(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution': 'Universidade Federal de Minas Gerais'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_name(
    client, create_patent, create_graduate_program, link_researcher_to_program
):
    # Arrange
    program = await create_graduate_program(name='Ciência da Computação')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'graduate_program': 'Ciência da Computação'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client, create_patent, create_graduate_program, link_researcher_to_program
):
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

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
    client, create_patent, create_researcher_production
):
    # Arrange
    researcher_prod = await create_researcher_production(city='Belo Horizonte')

    await create_patent()
    await create_patent(researcher_id=researcher_prod['researcher_id'])

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
    client, create_patent, create_researcher_production
):
    # Arrange
    areas = ['Ciências Exatas e da Terra', 'Engenharias']
    researcher_prod = await create_researcher_production(great_area_=areas)

    await create_patent()
    await create_patent(researcher_id=researcher_prod['researcher_id'])

    expected_count = 1
    params = {'area': 'Engenharias'}  # Filtra por uma das áreas

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(client, create_patent, create_foment):
    # Arrange
    foment = await create_foment(
        modality_name='Bolsa de Produtividade em Pesquisa'
    )

    await create_patent()
    await create_patent(researcher_id=foment['researcher_id'])

    expected_count = 1
    params = {'modality': 'Bolsa de Produtividade em Pesquisa'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(client, create_patent, create_researcher):
    # Arrange
    researcher = await create_researcher(graduation='Doutorado')

    await create_patent()
    await create_patent(researcher_id=researcher['id'])

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
    client, create_patent, create_researcher
):
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Patente com Título Repetido'
    await create_patent(title=common_title, researcher_id=researcher1['id'])
    await create_patent(title=common_title, researcher_id=researcher2['id'])
    await create_patent(title='Patente de Título Único')

    expected_count = 2
    params = {'distinct': 'true'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_patent):
    # Arrange:
    for i in range(5):
        await create_patent(title=f'Patente {i}')

    expected_count = 2
    params = {'page': '2', 'lenght': '2'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
