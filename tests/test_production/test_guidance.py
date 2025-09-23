from http import HTTPStatus

import pytest

# Define o endpoint-alvo para todos os testes neste arquivo
ENDPOINT_URL = '/production/guidance'


@pytest.mark.asyncio
async def test_get_all_guidances(client, create_guidance):
    """Testa a busca de todas as orientações, sem filtros."""
    # Arrange: Cria duas orientações distintas.
    await create_guidance()
    await create_guidance()
    expected_count = 2

    # Act: Realiza a requisição GET.
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert: Verifica se a resposta está OK e contém o número esperado de itens.
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title_and_student(client, create_guidance):
    # Arrange
    await create_guidance(title='Estudo sobre Deep Learning')
    await create_guidance(student_name='Maria da Silva')
    await create_guidance(title='Artigo aleatório')
    expected_count = 1

    # Act: Testa o termo no título
    response_title = client.get(ENDPOINT_URL, params={'term': 'Deep Learning'})
    data_title = response_title.json()

    # Assert
    assert response_title.status_code == HTTPStatus.OK
    assert len(data_title) == expected_count
    assert 'Deep Learning' in data_title[0]['title']


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client, create_guidance, create_graduate_program, link_researcher_to_program
):
    """Testa o filtro por ID do programa de pós-graduação."""
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_guidance()
    await create_guidance(researcher_id=linked['researcher_id'])
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
    client, create_guidance, create_graduate_program, link_researcher_to_program
):
    """Testa o filtro por nome do programa de pós-graduação."""
    # Arrange
    program = await create_graduate_program(
        name='Inteligência Artificial Aplicada'
    )
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_guidance()
    await create_guidance(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'graduate_program': 'Inteligência Artificial Aplicada'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_researcher_id(client, create_guidance):
    """Testa o filtro pelo ID do pesquisador (orientador)."""
    # Arrange
    await create_guidance()
    guidance_to_find = await create_guidance()
    expected_count = 1
    params = {'researcher_id': guidance_to_find['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client, create_guidance, create_department, link_researcher_to_department
):
    """Testa o filtro por ID do departamento."""
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_guidance()
    await create_guidance(researcher_id=linked['researcher_id'])
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
    client, create_guidance, create_department, link_researcher_to_department
):
    """Testa o filtro por nome do departamento."""
    # Arrange
    department = await create_department(dep_nom='Ciência da Computação')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_guidance()
    await create_guidance(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'departament': 'Ciência da Computação'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_year(client, create_guidance):
    """Testa o filtro por ano da orientação."""
    # Arrange
    await create_guidance(year=2022)
    await create_guidance(year=2024)
    expected_count = 1
    params = {'year': 2024}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_type(client, create_guidance):
    """Testa o filtro por tipo da orientação (status)."""
    # Arrange
    await create_guidance(type='ORIENTACAO_EM_ANDAMENTO')
    await create_guidance(type='ORIENTACAO_CONCLUIDA')
    expected_count = 1
    params = {'type': 'ORIENTACAO_CONCLUIDA'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_title(
    client, create_guidance, create_researcher
):
    """Testa o filtro 'distinct' para retornar orientações com títulos únicos."""
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Orientação com Título Repetido'
    await create_guidance(title=common_title, researcher_id=researcher1['id'])
    await create_guidance(title=common_title, researcher_id=researcher2['id'])
    await create_guidance(title='Orientação com Título Único')
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
    client, create_guidance, create_institution, create_researcher
):
    """Testa o filtro por nome da instituição."""
    # Arrange
    institution = await create_institution(
        name='Instituto Federal de Tecnologia'
    )
    researcher = await create_researcher(institution_id=institution['id'])
    await create_guidance()
    await create_guidance(researcher_id=researcher['id'])
    expected_count = 1
    params = {'institution': 'Instituto Federal de Tecnologia'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_city(
    client, create_guidance, create_researcher_production
):
    """Testa o filtro pela cidade de atuação do pesquisador."""
    # Arrange
    researcher_prod = await create_researcher_production(city='Feira de Santana')
    await create_guidance()
    await create_guidance(researcher_id=researcher_prod['researcher_id'])
    expected_count = 1
    params = {'city': 'Feira de Santana'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    client, create_guidance, create_researcher_production
):
    """Testa o filtro pela grande área de conhecimento."""
    # Arrange
    researcher_prod = await create_researcher_production(
        great_area_=['Ciências']
    )
    await create_guidance()
    await create_guidance(researcher_id=researcher_prod['researcher_id'])
    expected_count = 1
    params = {'area': 'Ciências'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(client, create_guidance, create_foment):
    """Testa o filtro pela modalidade de fomento do pesquisador."""
    # Arrange
    foment = await create_foment(modality_name='Apoio a Pesquisa Científica')
    await create_guidance()
    await create_guidance(researcher_id=foment['researcher_id'])
    expected_count = 1
    params = {'modality': 'Apoio a Pesquisa Científica'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(client, create_guidance, create_researcher):
    """Testa o filtro pelo nível de graduação do orientador."""
    # Arrange
    researcher = await create_researcher(graduation='DOUTORADO')
    await create_guidance()
    await create_guidance(researcher_id=researcher['id'])
    expected_count = 1
    params = {'graduation': 'DOUTORADO'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_lattes_id(client, create_guidance, create_researcher):
    """Testa o filtro pelo ID Lattes do pesquisador."""
    # Arrange
    target_lattes_id = '1122334455667788'
    researcher = await create_researcher(lattes_id=target_lattes_id)
    await create_guidance()
    await create_guidance(researcher_id=researcher['id'])
    expected_count = 1
    params = {'lattes_id': target_lattes_id}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_group_name(
    client,
    create_guidance,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome do grupo de pesquisa."""
    # Arrange
    group = await create_research_group(name='Grupo de Visão Computacional')
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_guidance()
    await create_guidance(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'group': 'Grupo de Visão Computacional'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_group_id(
    client,
    create_guidance,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por ID do grupo de pesquisa."""
    # Arrange
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_guidance()
    await create_guidance(researcher_id=linked['researcher_id'])
    expected_count = 1
    params = {'group_id': group['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_guidance):
    """Testa a paginação, usando os parâmetros 'page' e 'lenght'."""
    # Arrange: Cria 5 orientações para testar a paginação.
    for i in range(5):
        await create_guidance(title=f'Orientação {i + 1}')
    expected_count_page_2 = 2
    params = {'page': '2', 'lenght': '2'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert: Verifica se a segunda página com 2 itens por página funciona.
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count_page_2
    assert data[0]['title'] == 'Orientação 3'
    assert data[1]['title'] == 'Orientação 4'


@pytest.mark.asyncio
async def test_filter_by_institution_id(
    client, create_guidance, create_institution, create_researcher
):
    """Testa o filtro por ID da instituição do pesquisador."""
    # Arrange
    # Cria uma instituição e um pesquisador vinculado a ela
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    # Cria uma orientação "ruído" com um pesquisador de outra instituição
    await create_guidance()
    # Cria a orientação-alvo com o pesquisador da instituição correta
    await create_guidance(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution_id': institution['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_collection_id(
    client, create_guidance, create_collection_entry
):
    # Arrange
    # Cria algumas orientações que não estarão na coleção
    await create_guidance()
    await create_guidance()

    # Cria a orientação que será adicionada à coleção
    guidance_in_collection = await create_guidance()
    collection = await create_collection_entry(
        entry_id=guidance_in_collection['id'], type='GUIDANCE'
    )

    expected_count = 1
    params = {'collection_id': collection['collection_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_star(
    client,
    create_guidance,
    create_star_entry,
    override_get_current_user,
):
    await create_guidance()
    await create_guidance()

    stared_article = await create_guidance()

    star = await create_star_entry(
        entry_id=stared_article['id'],
        type='GUIDANCE',
    )

    override_get_current_user({'user_id': star['user_id']})

    expected_count = 1

    params = {'star': True}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
