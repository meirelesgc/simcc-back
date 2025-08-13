# tests/routes/test_researcher_book.py

from http import HTTPStatus

import pytest

from simcc.schemas import Researcher

ENDPOINT_URL = '/researcherBook'


@pytest.mark.asyncio
async def test_get_researcher_book(
    conn, client, create_bibliographic_production_book
):
    # Arrange
    expected_count = 2
    await create_bibliographic_production_book()
    await create_bibliographic_production_book()

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert [Researcher.Researcher(**item) for item in data]


@pytest.mark.asyncio
async def test_term_filter_on_book_title(
    client, create_bibliographic_production_book
):
    # Arrange
    target_title = 'A Ascensão da Inteligência Artificial'
    await create_bibliographic_production_book(
        title='Um Livro Sobre Outra Coisa'
    )
    await create_bibliographic_production_book(title=target_title)

    expected_count = 1
    params = {'term': 'Inteligência'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_year_filter_on_book(client, create_bibliographic_production_book):
    # Arrange
    await create_bibliographic_production_book(year=2010)
    await create_bibliographic_production_book(year=2023)

    expected_count = 1
    params = {'year': 2023}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduate_program_filter(
    client,
    create_bibliographic_production_book,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    graduate_program = await create_graduate_program()
    researcher_linked = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=researcher_linked['researcher_id']
    )

    expected_count = 1
    params = {'graduate_program': graduate_program['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_department_filter(
    client,
    create_bibliographic_production_book,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=link_info['researcher_id']
    )

    expected_count = 1
    params = {'departament': department['dep_nom']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_institution_filter(
    client,
    create_bibliographic_production_book,
    create_institution,
    create_researcher,
):
    """
    Testa o filtro por nome da instituição.
    """
    # Arrange
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    await create_bibliographic_production_book()
    await create_bibliographic_production_book(researcher_id=researcher['id'])

    expected_count = 1
    params = {'institution': institution['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_city_filter(
    client, create_bibliographic_production_book, create_researcher_production
):
    # Arrange
    city = 'Uma cidade qualquer'
    production_info = await create_researcher_production(city=city)

    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=production_info['researcher_id']
    )

    expected_count = 1
    params = {'city': city}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_area_filter(
    client, create_bibliographic_production_book, create_researcher_production
):
    # Arrange
    great_area_ = ['XPTO', 'SBPC']
    production_info = await create_researcher_production(great_area_=great_area_)
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=production_info['researcher_id']
    )

    expected_count = 1
    params = {'area': ';'.join(great_area_)}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_research_group_filter_by_name(
    client,
    create_bibliographic_production_book,
    create_research_group,
    link_researcher_to_research_group,
):
    """
    Testa o filtro por nome do grupo de pesquisa.
    """
    # Arrange
    research_group = await create_research_group()
    linked_researcher = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=linked_researcher['researcher_id']
    )

    expected_count = 1
    params = {'group': research_group['name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_modality_filter(
    client, create_bibliographic_production_book, create_foment
):
    # Arrange
    foment_info = await create_foment(
        modality_name='Bolsa de Produtividade em Pesquisa',
    )
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=foment_info['researcher_id']
    )

    expected_events_count = 1
    params = {'modality': foment_info['modality_name']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_researcher_id_filter(
    client, create_bibliographic_production_book
):
    # Arrange
    await create_bibliographic_production_book()
    book = await create_bibliographic_production_book()

    expected_events_count = 1
    params = {'researcher_id': book['bibliographic_production']['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_lattes_id_filter(
    client, create_bibliographic_production_book, create_researcher
):
    # Arrange
    lattes_id = 'XPTO'
    researcher = await create_researcher(lattes_id=lattes_id)
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(researcher_id=researcher['id'])

    expected_count = 1
    params = {'lattes_id': lattes_id}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduation_filter(
    client, create_bibliographic_production_book, create_researcher
):
    # Arrange
    graduation = 'Doutorado'
    researcher = await create_researcher(graduation=graduation)
    await create_bibliographic_production_book()
    await create_bibliographic_production_book(researcher_id=researcher['id'])

    expected_count = 1
    params = {'graduation': graduation}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_department_id_filter(
    client,
    create_bibliographic_production_book,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=link_info['researcher_id']
    )

    expected_events_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_graduate_program_id_filter(
    client,
    create_bibliographic_production_book,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    graduate_program = await create_graduate_program()
    researcher = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=researcher['researcher_id']
    )

    expected_events_count = 1
    params = {'graduate_program_id': graduate_program['graduate_program_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_research_group_filter_by_id(
    client,
    create_bibliographic_production_book,
    create_research_group,
    link_researcher_to_research_group,
):
    # Arrange
    research_group = await create_research_group()

    linked_researcher = await link_researcher_to_research_group(
        research_group_id=research_group['id']
    )

    await create_bibliographic_production_book()
    await create_bibliographic_production_book(
        researcher_id=linked_researcher['researcher_id']
    )

    expected_events_count = 1
    params = {'group_id': research_group['id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count
