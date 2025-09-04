# tests/test_researcher_production_papers_magazine.py

from http import HTTPStatus

import pytest
import pytest_asyncio

ENDPOINT_URL = '/production/paper'


@pytest_asyncio.fixture
def create_bibliographic_production_magazine(create_bibliographic_production):
    async def _create(**kwargs):
        kwargs['type'] = 'TEXT_IN_NEWSPAPER_MAGAZINE'
        return await create_bibliographic_production(**kwargs)

    return _create


@pytest.mark.asyncio
async def test_get_all_magazine_papers(
    client,
    create_bibliographic_production_magazine,
    create_bibliographic_production,
):
    """Testa a busca de todos os textos em jornais/revistas."""
    # Arrange
    # Cria um item do tipo correto e outro de um tipo diferente
    await create_bibliographic_production_magazine()
    await create_bibliographic_production(
        type='ARTICLE'
    )  # Este não deve aparecer
    expected_count = 1

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(
    client, create_bibliographic_production_magazine
):
    """Testa o filtro por um termo no título."""
    # Arrange
    target_title = 'O Impacto da IA na Sociedade Moderna'
    await create_bibliographic_production_magazine(
        title='Outro assunto qualquer'
    )
    await create_bibliographic_production_magazine(title=target_title)
    expected_count = 1
    params = {'term': 'Sociedade Moderna'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client,
    create_bibliographic_production_magazine,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por ID do programa de pós-graduação."""
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
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
    create_bibliographic_production_magazine,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por nome do programa de pós-graduação."""
    # Arrange
    program = await create_graduate_program(name='Comunicação Social')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=linked['researcher_id']
    )
    expected_count = 1
    params = {'graduate_program': 'Comunicação Social'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(
    client, create_bibliographic_production_magazine
):
    """Testa o filtro pelo ID do pesquisador."""
    # Arrange
    paper_to_find = await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine()
    expected_count = 1
    params = {'researcher_id': paper_to_find['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client,
    create_bibliographic_production_magazine,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por ID do departamento."""
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
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
    create_bibliographic_production_magazine,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por nome do departamento."""
    # Arrange
    department = await create_department(dep_nom='Jornalismo')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=linked['researcher_id']
    )
    expected_count = 1
    params = {'departament': 'Jornalismo'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_year(client, create_bibliographic_production_magazine):
    """Testa o filtro por ano de publicação."""
    # Arrange
    await create_bibliographic_production_magazine(year=2020)
    await create_bibliographic_production_magazine(year=2023)
    expected_count = 1
    params = {'year': 2023}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_title(
    client, create_bibliographic_production_magazine, create_researcher
):
    """Testa o filtro 'distinct' para retornar títulos únicos."""
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Título de Matéria Repetido'
    await create_bibliographic_production_magazine(
        title=common_title, researcher_id=researcher1['id']
    )
    await create_bibliographic_production_magazine(
        title=common_title, researcher_id=researcher2['id']
    )
    await create_bibliographic_production_magazine(title='Título Único')
    expected_count = 2
    params = {'distinct': 'true'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client,
    create_bibliographic_production_magazine,
    create_institution,
    create_researcher,
):
    """Testa o filtro por nome da instituição."""
    # Arrange
    institution = await create_institution(name='Escola de Comunicações e Artes')
    researcher = await create_researcher(institution_id=institution['id'])
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=researcher['id']
    )
    expected_count = 1
    params = {'institution': 'Escola de Comunicações e Artes'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_city(
    client,
    create_bibliographic_production_magazine,
    create_researcher_production,
):
    """Testa o filtro pela cidade do pesquisador."""
    # Arrange
    researcher_prod = await create_researcher_production(city='Porto Alegre')
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=researcher_prod['researcher_id']
    )
    expected_count = 1
    params = {'city': 'Porto Alegre'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    client,
    create_bibliographic_production_magazine,
    create_researcher_production,
):
    """Testa o filtro pela grande área de conhecimento."""
    # Arrange
    researcher_prod = await create_researcher_production(great_area_=['Letras'])
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=researcher_prod['researcher_id']
    )
    expected_count = 1
    params = {'area': 'Letras'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(
    client, create_bibliographic_production_magazine, create_foment
):
    """Testa o filtro pela modalidade de fomento."""
    # Arrange
    foment = await create_foment(
        modality_name='Bolsa de Produtividade em Pesquisa'
    )
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=foment['researcher_id']
    )
    expected_count = 1
    params = {'modality': 'Bolsa de Produtividade em Pesquisa'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(
    client, create_bibliographic_production_magazine, create_researcher
):
    """Testa o filtro pelo nível de graduação do pesquisador."""
    # Arrange
    researcher = await create_researcher(graduation='DOUTORADO')
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=researcher['id']
    )
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
    client, create_bibliographic_production_magazine, create_researcher
):
    """Testa o filtro pelo ID Lattes do pesquisador."""
    # Arrange
    target_lattes_id = '1212121212121212'
    researcher = await create_researcher(lattes_id=target_lattes_id)
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=researcher['id']
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
async def test_filter_by_group_id(
    client,
    create_bibliographic_production_magazine,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por ID do grupo de pesquisa."""
    # Arrange
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=linked['researcher_id']
    )
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
    create_bibliographic_production_magazine,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome do grupo de pesquisa."""
    # Arrange
    group = await create_research_group(name='Observatório da Mídia')
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine(
        researcher_id=linked['researcher_id']
    )
    expected_count = 1
    params = {'group': 'Observatório da Mídia'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_bibliographic_production_magazine):
    """Testa a paginação dos resultados."""
    # Arrange
    for i in range(5):
        await create_bibliographic_production_magazine(title=f'Matéria {i + 1}')
    expected_count = 2
    params = {'page': '2', 'lenght': '2'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['title'] == 'Matéria 3'


@pytest.mark.asyncio
async def test_collection_id(
    client,
    create_bibliographic_production_magazine,
    create_collection_entry,
):
    await create_bibliographic_production_magazine()
    await create_bibliographic_production_magazine()
    paper = await create_bibliographic_production_magazine()
    collection = await create_collection_entry(
        entry_id=paper['id'], type='PAPER'
    )
    expected_count = 1
    params = {'collection_id': collection['collection_id']}
    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
