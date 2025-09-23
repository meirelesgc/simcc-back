from http import HTTPStatus

import pytest

ENDPOINT_URL = '/production/professional-experience'


@pytest.mark.asyncio
async def test_get_all_experiences(
    client, create_researcher_professional_experience
):
    # Arrange
    await create_researcher_professional_experience()
    await create_researcher_professional_experience()
    expected_count = 2

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_year(
    conn, client, create_researcher_professional_experience
):
    # Arrange
    await create_researcher_professional_experience(end_year='2020')
    await create_researcher_professional_experience(end_year='2023')

    expected_count = 1
    params = {'year': '2022'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_department_name(
    client,
    create_researcher_professional_experience,
    create_department,
    link_researcher_to_department,
):
    """
    Testa o filtro pelo nome do departamento do pesquisador.
    """
    # Arrange
    department = await create_department(dep_nom='Engenharia de Software')
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await (
        create_researcher_professional_experience()
    )  # Experiência não relacionada
    await create_researcher_professional_experience(
        researcher_id=linked['researcher_id']
    )  # Experiência relacionada

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
    create_researcher_professional_experience,
    create_department,
    link_researcher_to_department,
):
    """
    Testa o filtro pelo ID do departamento do pesquisador.
    """
    # Arrange
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
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
    client, create_researcher_professional_experience
):
    """
    Testa o filtro direto pelo ID do pesquisador.
    """
    # Arrange
    await create_researcher_professional_experience()
    experience_to_find = await create_researcher_professional_experience()

    expected_count = 1
    params = {'researcher_id': experience_to_find['researcher_id']}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_name(
    client,
    create_researcher_professional_experience,
    create_institution,
    create_researcher,
):
    """
    Testa o filtro pelo nome da instituição do pesquisador.
    """
    # Arrange
    institution = await create_institution(
        name='Instituto Tecnológico de Aeronáutica'
    )
    researcher = await create_researcher(institution_id=institution['id'])

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
        researcher_id=researcher['id']
    )

    expected_count = 1
    params = {'institution': 'Instituto Tecnológico de Aeronáutica'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_name(
    client,
    create_researcher_professional_experience,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program(name='Engenharia Aeroespacial')
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
        researcher_id=linked['researcher_id']
    )

    expected_count = 1
    params = {'graduate_program': 'Engenharia Aeroespacial'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduate_program_id(
    client,
    create_researcher_professional_experience,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
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
    client,
    create_researcher_professional_experience,
    create_researcher_production,
):
    """
    Testa o filtro pela cidade de produção do pesquisador.
    """
    # Arrange
    researcher_prod = await create_researcher_production(
        city='São José dos Campos'
    )

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
        researcher_id=researcher_prod['researcher_id']
    )

    expected_count = 1
    params = {'city': 'São José dos Campos'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_area(
    client,
    create_researcher_professional_experience,
    create_researcher_production,
):
    """
    Testa o filtro pela grande área de atuação do pesquisador.
    """
    # Arrange
    areas = ['Ciências Exatas e da Terra', 'Engenharias']
    researcher_prod = await create_researcher_production(great_area_=areas)

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
        researcher_id=researcher_prod['researcher_id']
    )

    expected_count = 1
    params = {'area': 'Engenharias'}  # Filtra por uma das áreas

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_modality(
    client, create_researcher_professional_experience, create_foment
):
    """
    Testa o filtro pela modalidade de fomento do pesquisador.
    """
    # Arrange
    foment = await create_foment(
        modality_name='Bolsa de Desenvolvimento Tecnológico'
    )

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
        researcher_id=foment['researcher_id']
    )

    expected_count = 1
    params = {'modality': 'Bolsa de Desenvolvimento Tecnológico'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_graduation(
    client, create_researcher_professional_experience, create_researcher
):
    """
    Testa o filtro pela titulação do pesquisador.
    """
    # Arrange
    researcher = await create_researcher(graduation='Doutorado')

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
        researcher_id=researcher['id']
    )

    expected_count = 1
    params = {'graduation': 'Doutorado'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_distinct_on_enterprise(
    client, create_researcher_professional_experience, create_researcher
):
    # Arrange
    researcher1 = await create_researcher()
    common_enterprise = 'Empresa com Vários Vínculos'

    await create_researcher_professional_experience(
        enterprise=common_enterprise, researcher_id=researcher1['id']
    )
    await create_researcher_professional_experience(
        enterprise=common_enterprise, researcher_id=researcher1['id']
    )
    await create_researcher_professional_experience(
        enterprise='Empresa de Vínculo Único'
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
async def test_pagination(client, create_researcher_professional_experience):
    # Arrange:
    for i in range(5):
        await create_researcher_professional_experience(
            enterprise=f'Empresa {i}'
        )

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
    client, create_researcher_professional_experience, create_researcher
):
    # Arrange
    target_lattes_id = '1234567890123456'
    researcher = await create_researcher(lattes_id=target_lattes_id)

    await (
        create_researcher_professional_experience()
    )  # Patente de outro pesquisador
    await create_researcher_professional_experience(
        researcher_id=researcher['id']
    )  # Patente do pesquisador alvo

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
    create_researcher_professional_experience,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por ID do grupo de pesquisa do pesquisador."""
    # Arrange
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
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
    create_researcher_professional_experience,
    create_research_group,
    link_researcher_to_research_group,
):
    """Testa o filtro por nome do grupo de pesquisa do pesquisador."""
    # Arrange
    group = await create_research_group(
        name='Laboratório de Sistemas Embarcados'
    )
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
        researcher_id=linked['researcher_id']
    )

    expected_count = 1
    params = {'group': 'Laboratório de Sistemas Embarcados'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_filter_by_institution_id(
    client,
    create_researcher_professional_experience,
    create_institution,
    create_researcher,
):
    """Testa o filtro por ID da instituição do pesquisador."""
    # Arrange
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    await create_researcher_professional_experience()
    await create_researcher_professional_experience(
        researcher_id=researcher['id']
    )

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
    client, create_researcher_professional_experience, create_collection_entry
):
    """Testa o filtro por ID de uma coleção."""
    # Arrange
    await create_researcher_professional_experience()
    experience_in_collection = await create_researcher_professional_experience()

    collection = await create_collection_entry(
        entry_id=experience_in_collection['id'], type='PROFESSIONAL_EXPERIENCE'
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
    create_researcher_professional_experience,
    create_star_entry,
    override_get_current_user,
):
    await create_researcher_professional_experience()
    await create_researcher_professional_experience()

    stared_article = await create_researcher_professional_experience()

    star = await create_star_entry(
        entry_id=stared_article['id'],
        type='PROFESSIONAL_EXPERIENCE',
    )

    override_get_current_user({'user_id': star['user_id']})

    expected_count = 1

    params = {'star': True}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
