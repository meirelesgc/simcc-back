# tests/test_researcher_production_events.py

from http import HTTPStatus

import pytest

# Define o endpoint-alvo para todos os testes neste arquivo
ENDPOINT_URL = '/researcher_production/events'


@pytest.mark.asyncio
async def test_get_all_events(
    client, create_bibliographic_production_work_in_event
):
    """Testa a busca de todos os trabalhos em eventos, sem filtros."""
    # Arrange: Cria dois trabalhos distintos.
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event()
    expected_count = 2

    # Act: Realiza a requisição GET.
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert: Verifica se a resposta está OK e contém o número esperado de itens.
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(
    client, create_bibliographic_production_work_in_event
):
    """Testa o filtro por um termo no título do trabalho."""
    # Arrange
    target_title = 'Análise de Algoritmos de Machine Learning'
    await create_bibliographic_production_work_in_event(
        title='Um Estudo sobre Redes Neurais'
    )
    await create_bibliographic_production_work_in_event(title=target_title)
    expected_count = 1
    params = {'term': 'Machine Learning'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['title'] == target_title


@pytest.mark.asyncio
async def test_filter_by_year(
    client, create_bibliographic_production_work_in_event
):
    """Testa o filtro por ano (>=), retornando trabalhos a partir daquele ano."""
    # Arrange: Cria trabalhos em anos diferentes.
    await create_bibliographic_production_work_in_event(year=2020)
    await create_bibliographic_production_work_in_event(year=2023)
    await create_bibliographic_production_work_in_event(year=2024)

    # O filtro é "maior ou igual a", então 2023 e 2024 devem ser retornados.
    expected_count = 2
    params = {'year': 2023}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client,
    create_bibliographic_production_work_in_event,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por ID do departamento do autor."""
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=linked['researcher_id']
    )
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
    create_bibliographic_production_work_in_event,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por nome do departamento do autor."""
    # Arrange
    department = await create_department(dep_nom='Engenharia Elétrica')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=linked['researcher_id']
    )
    expected_count = 1
    params = {'departament': 'Engenharia Elétrica'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(
    client, create_bibliographic_production_work_in_event
):
    """Testa o filtro pelo ID do pesquisador (autor)."""
    # Arrange
    await create_bibliographic_production_work_in_event()
    event_to_find = await create_bibliographic_production_work_in_event()
    expected_count = 1
    params = {
        'researcher_id': event_to_find['bibliographic_production'][
            'researcher_id'
        ]
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client,
    create_bibliographic_production_work_in_event,
    create_institution,
    create_researcher,
):
    """Testa o filtro por nome da instituição do autor."""
    # Arrange
    institution = await create_institution(
        name='Universidade Estadual de Campinas'
    )
    researcher = await create_researcher(institution_id=institution['id'])
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=researcher['id']
    )
    expected_count = 1
    params = {'institution': 'Universidade Estadual de Campinas'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client,
    create_bibliographic_production_work_in_event,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por ID do programa de pós-graduação."""
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=linked['researcher_id']
    )
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
    create_bibliographic_production_work_in_event,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por nome do programa de pós-graduação."""
    # Arrange
    program = await create_graduate_program(name='Física Aplicada')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=linked['researcher_id']
    )
    expected_count = 1
    params = {'graduate_program': 'Física Aplicada'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_city(
    client,
    create_bibliographic_production_work_in_event,
    create_researcher_production,
):
    """Testa o filtro pela cidade de atuação do autor."""
    # Arrange
    researcher_prod = await create_researcher_production(city='Belo Horizonte')
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=researcher_prod['researcher_id']
    )
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
    client,
    create_bibliographic_production_work_in_event,
    create_researcher_production,
):
    """Testa o filtro pela grande área de conhecimento do autor."""
    # Arrange
    researcher_prod = await create_researcher_production(great_area_=['Sociais'])
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=researcher_prod['researcher_id']
    )
    expected_count = 1
    params = {'area': 'Sociais'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(
    client, create_bibliographic_production_work_in_event, create_foment
):
    """Testa o filtro pela modalidade de fomento do autor."""
    # Arrange
    foment = await create_foment(modality_name='Bolsas de Iniciação Científica')
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=foment['researcher_id']
    )
    expected_count = 1
    params = {'modality': 'Bolsas de Iniciação Científica'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(
    client, create_bibliographic_production_work_in_event, create_researcher
):
    """Testa o filtro pelo nível de graduação do autor."""
    # Arrange
    researcher = await create_researcher(graduation='MESTRADO')
    await create_bibliographic_production_work_in_event()
    await create_bibliographic_production_work_in_event(
        researcher_id=researcher['id']
    )
    expected_count = 1
    params = {'graduation': 'MESTRADO'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_title(
    client, create_bibliographic_production_work_in_event, create_researcher
):
    """Testa o filtro 'distinct' para retornar trabalhos com títulos únicos."""
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Trabalho com Título Repetido'
    await create_bibliographic_production_work_in_event(
        title=common_title, researcher_id=researcher1['id']
    )
    await create_bibliographic_production_work_in_event(
        title=common_title, researcher_id=researcher2['id']
    )
    await create_bibliographic_production_work_in_event(
        title='Trabalho com Título Único'
    )
    expected_count = 2
    params = {'distinct': 'true'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_bibliographic_production_work_in_event):
    """Testa a paginação, usando os parâmetros 'page' e 'lenght'."""
    # Arrange: Cria 5 trabalhos para testar a paginação.
    for i in range(5):
        await create_bibliographic_production_work_in_event(
            title=f'Evento {i + 1}'
        )
    expected_count_page_2 = 2
    params = {'page': '2', 'lenght': '2'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert: Verifica se a segunda página com 2 itens por página funciona.
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count_page_2
