from http import HTTPStatus

import pytest

ENDPOINT_URL = '/production/book'


@pytest.mark.asyncio
async def test_get_all_books(client, create_bibliographic_production_book):
    # Arrange
    await create_bibliographic_production_book()
    await create_bibliographic_production_book()
    expected_count = 2

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_term_on_title(
    client, create_bibliographic_production_book
):
    # Arrange
    target_title = 'A História da Computação'
    await create_bibliographic_production_book(title='Outro Livro Aleatório')
    await create_bibliographic_production_book(title=target_title)
    expected_count = 1
    params = {'term': 'Computação'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_year(client, create_bibliographic_production_book):
    # Arrange
    await create_bibliographic_production_book(year=2020)
    await create_bibliographic_production_book(year=2023)
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
    create_bibliographic_production_book,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department(dep_nom='Engenharia de Software')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=linked['researcher_id']
    )
    expected_count = 1
    params = {'departament': 'Engenharia de Software'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_id(
    client,
    create_bibliographic_production_book,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
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
async def test_filter_by_researcher_id(
    client, create_bibliographic_production_book
):
    # Arrange
    await create_bibliographic_production_book()
    book_to_find = await create_bibliographic_production_book()
    expected_count = 1
    params = {
        'researcher_id': book_to_find['bibliographic_production'][
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
    create_bibliographic_production_book,
    create_institution,
    create_researcher,
):
    # Arrange
    institution = await create_institution(
        name='Instituto de Tecnologia Avançada'
    )
    researcher = await create_researcher(institution_id=institution['id'])
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(researcher_id=researcher['id'])
    expected_count = 1
    params = {'institution': 'Instituto de Tecnologia Avançada'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_name(
    client,
    create_bibliographic_production_book,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program(name='Inteligência Artificial')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=linked['researcher_id']
    )
    expected_count = 1
    params = {'graduate_program': 'Inteligência Artificial'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client,
    create_bibliographic_production_book,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
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
async def test_filter_by_city(
    client, create_bibliographic_production_book, create_researcher_production
):
    # Arrange
    researcher_prod = await create_researcher_production(city='Campinas')
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=researcher_prod['researcher_id']
    )
    expected_count = 1
    params = {'city': 'Campinas'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    client, create_bibliographic_production_book, create_researcher_production
):
    # Arrange
    areas = ['Ciências Biológicas', 'Saúde']
    researcher_prod = await create_researcher_production(great_area_=areas)
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=researcher_prod['researcher_id']
    )
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
    client, create_bibliographic_production_book, create_foment
):
    # Arrange
    foment = await create_foment(modality_name='Apoio a Projetos de Pesquisa')
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=foment['researcher_id']
    )
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
    client, create_bibliographic_production_book, create_researcher
):
    # Arrange
    researcher = await create_researcher(graduation='Mestrado')
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(researcher_id=researcher['id'])
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
    client, create_bibliographic_production_book, create_researcher
):
    # Arrange
    researcher1 = await create_researcher()
    researcher2 = await create_researcher()
    common_title = 'Livro com Título Repetido'
    await create_bibliographic_production_book(
        title=common_title, researcher_id=researcher1['id']
    )
    await create_bibliographic_production_book(
        title=common_title, researcher_id=researcher2['id']
    )
    await create_bibliographic_production_book(title='Livro de Título Único')
    expected_count = 2
    params = {'distinct': 'true'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_pagination(client, create_bibliographic_production_book):
    # Arrange
    for i in range(5):
        await create_bibliographic_production_book(title=f'Livro {i}')
    expected_count = 2
    params = {'page': '2', 'lenght': '2'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_lattes_id(
    client, create_bibliographic_production_book, create_researcher
):
    # Arrange
    target_lattes_id = '9876543210987654'
    researcher = await create_researcher(lattes_id=target_lattes_id)
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(researcher_id=researcher['id'])
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
    create_bibliographic_production_book,
    create_research_group,
    link_researcher_to_research_group,
):
    # Arrange
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
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
async def test_filter_by_institution_id(
    client,
    create_bibliographic_production_book,
    create_institution,
    create_researcher,
):
    """Testa o filtro por ID da instituição do pesquisador."""
    # Arrange
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(researcher_id=researcher['id'])
    expected_count = 1
    params = {'institution_id': institution['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_group_name(
    client,
    create_bibliographic_production_book,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome do grupo de pesquisa."""
    # Arrange
    group = await create_research_group(name='Estudos de Mídia Digital')
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=linked['researcher_id']
    )
    expected_count = 1
    params = {'group': 'Estudos de Mídia Digital'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_collection_id(
    client, create_bibliographic_production_book, create_collection_entry
):
    """Testa o filtro por ID de uma coleção."""
    # Arrange
    await create_bibliographic_production_book()
    book_in_collection = await create_bibliographic_production_book()
    # Acessa o ID correto dentro da estrutura aninhada, se necessário
    entry_id = book_in_collection['bibliographic_production']['id']

    collection = await create_collection_entry(entry_id=entry_id, type='BOOK')

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
    create_bibliographic_production_book,
    create_star_entry,
    override_get_current_user,
):
    await create_bibliographic_production_book()
    await create_bibliographic_production_book()

    stared_article = await create_bibliographic_production_book()

    star = await create_star_entry(
        entry_id=stared_article['bibliographic_production']['id'],
        type='BOOK',
    )

    override_get_current_user({'user_id': star['user_id']})

    expected_count = 1

    params = {'star': True}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
