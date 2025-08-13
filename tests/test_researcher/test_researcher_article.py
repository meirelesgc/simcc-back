# tests/routes/test_researcher_article.py

from http import HTTPStatus

import pytest

from simcc.schemas import Researcher

# O endpoint para pesquisadores, o tipo será especificado nos parâmetros
ENDPOINT_URL = '/researcher'


@pytest.mark.asyncio
async def test_get_researcher_article(
    conn, client, create_bibliographic_production_article
):
    """Testa a listagem de produções do tipo ARTIGO."""
    # Arrange
    expected_count = 2
    # Cria duas produções de artigo
    await create_bibliographic_production_article()
    await create_bibliographic_production_article()

    # Parâmetros para filtrar apenas artigos
    params = {'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert [Researcher.Researcher(**item) for item in data]


@pytest.mark.asyncio
async def test_term_filter_on_article_title(
    client, create_bibliographic_production_article
):
    """Testa o filtro por termo no título de um artigo."""
    # Arrange
    target_title = 'A Evolução dos Algoritmos de IA'
    # Cria um artigo com um título diferente
    await create_bibliographic_production_article(
        title='Um Artigo Sobre Outro Assunto'
    )
    # Cria o artigo alvo
    await create_bibliographic_production_article(title=target_title)

    expected_count = 1
    # Parâmetros com o termo de busca e o tipo de produção
    params = {'term': 'Algoritmos', 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_year_filter_on_article(
    client, create_bibliographic_production_article
):
    """Testa o filtro por ano em um artigo."""
    # Arrange
    await create_bibliographic_production_article(year=2015)
    await create_bibliographic_production_article(year=2024)

    expected_count = 1
    params = {'year': 2024, 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduate_program_filter(
    client,
    create_bibliographic_production_article,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por programa de pós-graduação."""
    # Arrange
    graduate_program = await create_graduate_program()
    researcher_linked = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    # Cria um artigo com um pesquisador não vinculado
    await create_bibliographic_production_article()
    # Cria um artigo com o pesquisador vinculado ao programa
    await create_bibliographic_production_article(
        researcher_id=researcher_linked['researcher_id']
    )

    expected_count = 1
    params = {
        'graduate_program': graduate_program['name'],
        'type': 'ARTICLE',
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_department_filter(
    client,
    create_bibliographic_production_article,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por nome do departamento."""
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_bibliographic_production_article()
    await create_bibliographic_production_article(
        researcher_id=link_info['researcher_id']
    )

    expected_count = 1
    params = {'departament': department['dep_nom'], 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_institution_filter(
    client,
    create_bibliographic_production_article,
    create_institution,
    create_researcher,
):
    """Testa o filtro por nome da instituição."""
    # Arrange
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    await create_bibliographic_production_article()
    await create_bibliographic_production_article(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution': institution['name'], 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_city_filter(
    client, create_bibliographic_production_article, create_researcher_production
):
    """Testa o filtro por cidade."""
    # Arrange
    city = 'Uma cidade qualquer'
    production_info = await create_researcher_production(city=city)

    await create_bibliographic_production_article()
    await create_bibliographic_production_article(
        researcher_id=production_info['researcher_id']
    )

    expected_count = 1
    params = {'city': city, 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_area_filter(
    client, create_bibliographic_production_article, create_researcher_production
):
    """Testa o filtro por grande área de conhecimento."""
    # Arrange
    great_area_ = ['Ciências Exatas', 'Engenharias']
    production_info = await create_researcher_production(great_area_=great_area_)
    await create_bibliographic_production_article()
    await create_bibliographic_production_article(
        researcher_id=production_info['researcher_id']
    )

    expected_count = 1
    params = {'area': ';'.join(great_area_), 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_research_group_filter_by_name(
    client,
    create_bibliographic_production_article,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome do grupo de pesquisa."""
    # Arrange
    research_group = await create_research_group()
    linked_researcher = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    await create_bibliographic_production_article()
    await create_bibliographic_production_article(
        researcher_id=linked_researcher['researcher_id']
    )

    expected_count = 1
    params = {'group': research_group['name'], 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_modality_filter(
    client, create_bibliographic_production_article, create_foment
):
    """Testa o filtro por modalidade de fomento."""
    # Arrange
    foment_info = await create_foment(
        modality_name='Bolsa de Iniciação Científica',
    )
    await create_bibliographic_production_article()
    await create_bibliographic_production_article(
        researcher_id=foment_info['researcher_id']
    )

    expected_count = 1
    params = {'modality': foment_info['modality_name'], 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_researcher_id_filter(
    client, create_bibliographic_production_article
):
    """Testa o filtro por ID do pesquisador."""
    # Arrange
    await create_bibliographic_production_article()
    article = await create_bibliographic_production_article()

    expected_count = 1
    params = {
        'researcher_id': article['bibliographic_production']['researcher_id'],
        'type': 'ARTICLE',
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_lattes_id_filter(
    client, create_bibliographic_production_article, create_researcher
):
    """Testa o filtro por ID Lattes do pesquisador."""
    # Arrange
    lattes_id = '1234567890123456'
    researcher = await create_researcher(lattes_id=lattes_id)
    await create_bibliographic_production_article()
    await create_bibliographic_production_article(researcher_id=researcher['id'])

    expected_count = 1
    params = {'lattes_id': lattes_id, 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduation_filter(
    client, create_bibliographic_production_article, create_researcher
):
    """Testa o filtro por nível de graduação do pesquisador."""
    # Arrange
    graduation = 'Doutorado'
    researcher = await create_researcher(graduation=graduation)
    await create_bibliographic_production_article()
    await create_bibliographic_production_article(researcher_id=researcher['id'])

    expected_count = 1
    params = {'graduation': graduation, 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_department_id_filter(
    client,
    create_bibliographic_production_article,
    create_department,
    link_researcher_to_department,
):
    """Testa o filtro por ID do departamento."""
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_bibliographic_production_article()
    await create_bibliographic_production_article(
        researcher_id=link_info['researcher_id']
    )

    expected_count = 1
    params = {'dep_id': department['dep_id'], 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduate_program_id_filter(
    client,
    create_bibliographic_production_article,
    create_graduate_program,
    link_researcher_to_program,
):
    """Testa o filtro por ID do programa de pós-graduação."""
    # Arrange
    graduate_program = await create_graduate_program()
    researcher = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_bibliographic_production_article()
    await create_bibliographic_production_article(
        researcher_id=researcher['researcher_id']
    )

    expected_count = 1
    params = {
        'graduate_program_id': graduate_program['graduate_program_id'],
        'type': 'ARTICLE',
    }

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_research_group_filter_by_id(
    client,
    create_bibliographic_production_article,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por ID do grupo de pesquisa."""
    # Arrange
    research_group = await create_research_group()
    linked_researcher = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    await create_bibliographic_production_article()
    await create_bibliographic_production_article(
        researcher_id=linked_researcher['researcher_id']
    )

    expected_count = 1
    params = {'group_id': research_group['id'], 'type': 'ARTICLE'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
