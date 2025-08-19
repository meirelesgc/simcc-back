# tests/test_researcher_report.py

from http import HTTPStatus

import pytest

ENDPOINT_URL = '/researcher_report'


@pytest.mark.asyncio
async def test_get_all_reports(client, create_research_report):
    # Arrange
    await create_research_report()
    await create_research_report()
    expected_count = 2

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(client, create_research_report):
    # Arrange
    target_title = 'Relatório sobre Inteligência Artificial'
    await create_research_report(title='Outro Relatório Aleatório')
    await create_research_report(title=target_title)

    expected_count = 1
    params = {'term': 'Inteligência Artificial'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['title'] == target_title


@pytest.mark.asyncio
async def test_filter_by_year(client, create_research_report):
    # Arrange
    await create_research_report(year='2021')
    await create_research_report(year='2023')

    expected_count = 1
    params = {'year': '2023'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_name(
    client,
    create_research_report,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department(dep_nom='Ciência da Computação')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_research_report()
    await create_research_report(researcher_id=linked['researcher_id'])

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
    create_research_report,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_research_report()
    await create_research_report(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(client, create_research_report):
    # Arrange
    await create_research_report()
    report_to_find = await create_research_report()

    expected_count = 1
    params = {'researcher_id': report_to_find['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client, create_research_report, create_institution, create_researcher
):
    # Arrange
    institution = await create_institution(name='Universidade de São Paulo')
    researcher = await create_researcher(institution_id=institution['id'])

    await create_research_report()
    await create_research_report(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution': 'Universidade de São Paulo'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_name(
    client,
    create_research_report,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program(name='Engenharia de Software')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_research_report()
    await create_research_report(researcher_id=linked['researcher_id'])

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
    create_research_report,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_research_report()
    await create_research_report(researcher_id=linked['researcher_id'])

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
    client, create_research_report, create_researcher_production
):
    # Arrange
    researcher_prod = await create_researcher_production(city='Salvador')

    await create_research_report()
    await create_research_report(researcher_id=researcher_prod['researcher_id'])

    expected_count = 1
    params = {'city': 'Salvador'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    client, create_research_report, create_researcher_production
):
    # Arrange
    areas = ['Ciências Biológicas', 'Saúde']
    researcher_prod = await create_researcher_production(great_area_=areas)

    await create_research_report()
    await create_research_report(researcher_id=researcher_prod['researcher_id'])

    expected_count = 1
    params = {'area': 'Saúde'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(client, create_research_report, create_foment):
    # Arrange
    foment = await create_foment(modality_name='Bolsa de Iniciação Científica')

    await create_research_report()
    await create_research_report(researcher_id=foment['researcher_id'])

    expected_count = 1
    params = {'modality': 'Bolsa de Iniciação Científica'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(
    client, create_research_report, create_researcher
):
    # Arrange
    researcher = await create_researcher(graduation='Mestrado')

    await create_research_report()
    await create_research_report(researcher_id=researcher['id'])

    expected_count = 1
    params = {'graduation': 'Mestrado'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_title(
    client, create_research_report, create_researcher
):
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Relatório com Título Repetido'
    await create_research_report(
        title=common_title, researcher_id=researcher1['id']
    )
    await create_research_report(
        title=common_title, researcher_id=researcher2['id']
    )
    await create_research_report(title='Relatório de Título Único')

    expected_count = 2
    params = {'distinct': 'true'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_research_report):
    # Arrange
    for i in range(7):
        await create_research_report(title=f'Relatório {i}')

    expected_count = 3
    params = {'page': '2', 'lenght': '3'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_lattes_id(
    client, create_research_report, create_researcher
):
    # Arrange
    target_lattes_id = '9876543210987654'
    researcher = await create_researcher(lattes_id=target_lattes_id)

    await create_research_report()
    await create_research_report(researcher_id=researcher['id'])

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
    create_research_report,
    create_research_group,
    link_researcher_to_research_group,
):
    # Arrange
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )

    await create_research_report()
    await create_research_report(researcher_id=linked['researcher_id'])

    expected_count = 1
    params = {'group_id': group['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
