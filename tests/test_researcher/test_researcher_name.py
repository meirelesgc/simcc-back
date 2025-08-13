from http import HTTPStatus

import pytest

from simcc.schemas import Researcher

ENDPOINT_URL = '/researcherName'


@pytest.mark.asyncio
async def test_get_researcher_by_abstract_type(client, create_researcher):
    """Testa a listagem de pesquisadores usando o tipo ABSTRACT."""
    # Arrange
    await create_researcher()
    await create_researcher()
    expected_count = 2
    params = {}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert [Researcher.Researcher(**item) for item in data]


@pytest.mark.asyncio
async def test_term_filter_on_researcher_abstract(client, create_researcher):
    """Testa o filtro por termo no resumo (abstract) de um pesquisador."""
    # Arrange
    target_abstract = (
        'Este pesquisador estuda a sinergia quântica em sistemas complexos.'
    )
    await create_researcher(abstract='Um resumo sobre outro tópico.')
    target_researcher = await create_researcher(abstract=target_abstract)

    expected_count = 1
    params = {
        'term': 'sinergia quântica',
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == target_researcher['id']


@pytest.mark.asyncio
async def test_name_filter_on_researcher_name(client, create_researcher):
    target_name = 'Dr. Albert Sabin'
    await create_researcher(name='Dra. Marie Curie')
    target_researcher = await create_researcher(name=target_name)

    expected_count = 1
    params = {'name': 'Sabin'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == target_researcher['id']


@pytest.mark.asyncio
async def test_graduate_program_filter_on_abstract_search(
    client,
    create_researcher,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por programa de pós-graduação na busca por resumo."""
    # Arrange
    await create_researcher()  # Pesquisador de controle, não vinculado
    program = await create_graduate_program()
    link_info = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    expected_count = 1
    params = {
        'graduate_program': program['name'],
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == link_info['researcher_id']


@pytest.mark.asyncio
async def test_department_filter_on_abstract_search(
    client, create_researcher, create_department, link_researcher_to_department
):
    """Testa o filtro por departamento na busca por resumo."""
    # Arrange
    await create_researcher()  # Pesquisador de controle
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    expected_count = 1
    params = {
        'departament': department['dep_nom'],
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == link_info['researcher_id']


@pytest.mark.asyncio
async def test_institution_filter_on_abstract_search(
    client, create_researcher, create_institution
):
    """Testa o filtro por instituição na busca por resumo."""
    # Arrange
    await create_researcher()  # Pesquisador com instituição padrão
    institution = await create_institution(name='Universidade de Marte')
    target_researcher = await create_researcher(institution_id=institution['id'])

    expected_count = 1
    params = {
        'institution': institution['name'],
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == target_researcher['id']


@pytest.mark.asyncio
async def test_city_filter_on_abstract_search(
    client, create_researcher, create_researcher_production
):
    """Testa o filtro por cidade na busca por resumo."""
    # Arrange
    city = 'Belo Horizonte'
    await create_researcher()  # Pesquisador de controle
    target_researcher = await create_researcher()
    await create_researcher_production(
        researcher_id=target_researcher['id'], city=city
    )

    expected_count = 1
    params = {
        'city': city,
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == target_researcher['id']


@pytest.mark.asyncio
async def test_area_filter(
    client, create_researcher, create_researcher_production
):
    # Arrange
    great_area_ = ['XPTO', 'SBPC']
    production_info = await create_researcher_production(great_area_=great_area_)
    await create_researcher_production()
    await create_researcher_production(
        researcher_id=production_info['researcher_id']
    )

    expected_count = 1
    params = {
        'area': ';'.join(great_area_),
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_research_group_filter_by_name_on_abstract_search(
    client,
    create_researcher,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome de grupo de pesquisa na busca por resumo."""
    # Arrange
    await create_researcher()  # Pesquisador de controle
    research_group = await create_research_group()
    link_info = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    expected_count = 1
    params = {
        'group': research_group['name'],
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == link_info['researcher_id']


@pytest.mark.asyncio
async def test_modality_filter_on_abstract_search(
    client, create_researcher, create_foment
):
    """Testa o filtro por modalidade de fomento na busca por resumo."""
    # Arrange
    modality = 'Bolsa de Produtividade em Pesquisa'
    await create_researcher()  # Pesquisador de controle
    foment_info = await create_foment(modality_name=modality)

    expected_count = 1
    params = {
        'modality': modality,
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == foment_info['researcher_id']


@pytest.mark.asyncio
async def test_researcher_id_filter_on_abstract_search(
    client, create_researcher
):
    """Testa o filtro por ID do pesquisador na busca por resumo."""
    # Arrange
    await create_researcher()
    target_researcher = await create_researcher()

    expected_count = 1
    params = {
        'researcher_id': target_researcher['id'],
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == target_researcher['id']


@pytest.mark.asyncio
async def test_lattes_id_filter_on_abstract_search(client, create_researcher):
    """Testa o filtro por ID Lattes na busca por resumo."""
    # Arrange
    lattes_id = '1122334455667788'
    await create_researcher()
    target_researcher = await create_researcher(lattes_id=lattes_id)

    expected_count = 1
    params = {
        'lattes_id': lattes_id,
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == target_researcher['id']


@pytest.mark.asyncio
async def test_graduation_filter_on_abstract_search(client, create_researcher):
    """Testa o filtro por formação na busca por resumo."""
    # Arrange
    graduation = 'Doutorado'
    await create_researcher(graduation='Mestrado')
    target_researcher = await create_researcher(graduation=graduation)

    expected_count = 1
    params = {
        'graduation': graduation,
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == target_researcher['id']


# Adicionando testes para os filtros por ID que faltavam


@pytest.mark.asyncio
async def test_department_id_filter_on_abstract_search(
    client, create_researcher, create_department, link_researcher_to_department
):
    """Testa o filtro por ID de departamento na busca por resumo."""
    # Arrange
    await create_researcher()  # Pesquisador de controle
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    expected_count = 1
    params = {
        'dep_id': department['dep_id'],
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == link_info['researcher_id']


@pytest.mark.asyncio
async def test_graduate_program_id_filter_on_abstract_search(
    client,
    create_researcher,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por ID de programa de pós-graduação na busca por resumo."""
    # Arrange
    await create_researcher()  # Pesquisador de controle
    program = await create_graduate_program()
    link_info = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    expected_count = 1
    params = {
        'graduate_program_id': program['graduate_program_id'],
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == link_info['researcher_id']


@pytest.mark.asyncio
async def test_research_group_id_filter_on_abstract_search(
    client,
    create_researcher,
    create_research_group,
    link_researcher_to_research_group,
):
    # Arrange
    await create_researcher()
    research_group = await create_research_group()
    link_info = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    expected_count = 1
    params = {
        'group_id': research_group['id'],
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert data[0]['id'] == link_info['researcher_id']
