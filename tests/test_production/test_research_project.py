# tests/test_researcher_research_project.py

from http import HTTPStatus

import pytest

# Define o endpoint-alvo para todos os testes neste arquivo
ENDPOINT_URL = '/researcher_research_project'


@pytest.mark.asyncio
async def test_get_all_projects(client, create_research_project):
    """Testa a busca de todos os projetos de pesquisa, sem filtros."""
    # Arrange: Cria dois projetos distintos.
    await create_research_project()
    await create_research_project()
    expected_count = 2

    # Act: Realiza a requisição GET.
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert: Verifica se a resposta está OK e contém o número esperado de itens.
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(client, create_research_project):
    """Testa o filtro por um termo no título do projeto."""
    # Arrange
    target_title = 'Projeto de Inovação em Biotecnologia'
    await create_research_project(
        project_name='Pesquisa sobre energias renováveis'
    )
    await create_research_project(project_name=target_title)
    expected_count = 1
    params = {'term': 'Biotecnologia'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['project_name'] == target_title


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client,
    create_research_project,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por ID do programa de pós-graduação do pesquisador."""
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_research_project()
    await create_research_project(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'graduate_program_id': program['graduate_program_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_name(
    client,
    create_research_project,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por nome do programa de pós-graduação do pesquisador."""
    # Arrange
    program = await create_graduate_program(name='Medicina Tropical')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_research_project()
    await create_research_project(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'graduate_program': 'Medicina Tropical'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(client, create_research_project):
    """Testa o filtro pelo ID de um pesquisador participante."""
    # Arrange
    project_to_find = await create_research_project()
    await create_research_project()
    expected_count = 1
    params = {'researcher_id': project_to_find['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client,
    create_research_project,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por ID do departamento do pesquisador."""
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_research_project()
    await create_research_project(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_name(
    client,
    create_research_project,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por nome do departamento do pesquisador."""
    # Arrange
    department = await create_department(dep_nom='Letras e Linguística')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_research_project()
    await create_research_project(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'departament': 'Letras e Linguística'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_year_in_range(client, create_research_project):
    """Testa o filtro por ano, verificando se o ano está dentro da vigência do projeto."""
    # Arrange
    await create_research_project(
        start_year=2018, end_year=2020
    )  # Fora do range
    await create_research_project(start_year=2022, end_year=2024)  # Alvo
    expected_count = 1
    params = {'year': 2022}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['start_year'] <= 2022 <= data[0]['end_year']


@pytest.mark.asyncio
async def test_filter_by_type(client, create_research_project):
    """Testa o filtro por tipo/natureza do projeto."""
    # Arrange
    await create_research_project(nature='EXTENSAO')
    await create_research_project(nature='PESQUISA')
    expected_count = 1
    params = {'type': 'PESQUISA'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_title(
    client, create_research_project, create_researcher
):
    """Testa o filtro 'distinct' para retornar projetos com títulos únicos."""
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Projeto com Título Repetido'
    await create_research_project(
        project_name=common_title, researcher_id=researcher1['id']
    )
    await create_research_project(
        project_name=common_title, researcher_id=researcher2['id']
    )
    await create_research_project(project_name='Projeto com Título Único')
    expected_count = 2
    params = {'distinct': 1}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client, create_research_project, create_institution, create_researcher
):
    """Testa o filtro por nome da instituição do pesquisador."""
    # Arrange
    institution = await create_institution(name='Fundação Oswaldo Cruz')
    researcher = await create_researcher(institution_id=institution['id'])
    await create_research_project()
    await create_research_project(researcher_id=researcher['id'])
    expected_count = 1
    params = {'institution': 'Fundação Oswaldo Cruz'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_city(
    client, create_research_project, create_researcher_production
):
    """Testa o filtro pela cidade de atuação do pesquisador."""
    # Arrange
    researcher_prod = await create_researcher_production(city='Recife')
    await create_research_project()
    await create_research_project(researcher_id=researcher_prod['researcher_id'])
    expected_count = 1
    params = {'city': 'Recife'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    client, create_research_project, create_researcher_production
):
    """Testa o filtro pela grande área de conhecimento do pesquisador."""
    # Arrange
    researcher_prod = await create_researcher_production(great_area_=['Saúde'])
    await create_research_project()
    await create_research_project(researcher_id=researcher_prod['researcher_id'])
    expected_count = 1
    params = {'area': 'Saúde'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(
    client, create_research_project, create_foment
):
    """Testa o filtro pela modalidade de fomento do pesquisador."""
    # Arrange
    foment = await create_foment(modality_name='Apoio a Projetos de Pesquisa')
    await create_research_project()
    await create_research_project(researcher_id=foment['researcher_id'])
    expected_count = 1
    params = {'modality': 'Apoio a Projetos de Pesquisa'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(
    client, create_research_project, create_researcher
):
    """Testa o filtro pelo nível de graduação do pesquisador."""
    # Arrange
    researcher = await create_researcher(graduation='DOUTORADO')
    await create_research_project()
    await create_research_project(researcher_id=researcher['id'])
    expected_count = 1
    params = {'graduation': 'DOUTORADO'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_lattes_id(
    client, create_research_project, create_researcher
):
    """Testa o filtro pelo ID Lattes do pesquisador."""
    # Arrange
    target_lattes_id = '9988776655443322'
    researcher = await create_researcher(lattes_id=target_lattes_id)
    await create_research_project()
    await create_research_project(researcher_id=researcher['id'])
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
    create_research_project,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por ID do grupo de pesquisa."""
    # Arrange
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_research_project()
    await create_research_project(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'group_id': group['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_group_name(
    client,
    create_research_project,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome do grupo de pesquisa."""
    # Arrange
    group = await create_research_group(
        name='Laboratório de Sistemas Distribuídos'
    )
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_research_project()
    await create_research_project(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'group': 'Laboratório de Sistemas Distribuídos'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_research_project):
    """Testa a paginação, usando os parâmetros 'page' e 'lenght'."""
    # Arrange: Cria 5 projetos para testar a paginação.
    for i in range(5):
        await create_research_project(project_name=f'Projeto {i + 1}')
    expected_count_page_2 = 2
    params = {'page': '2', 'lenght': '2'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert: Verifica se a segunda página com 2 itens por página funciona.
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count_page_2
    assert data[0]['project_name'] == 'Projeto 3'
    assert data[1]['project_name'] == 'Projeto 4'
