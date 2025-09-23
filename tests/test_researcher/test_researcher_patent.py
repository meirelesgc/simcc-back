from http import HTTPStatus

import pytest

from simcc.schemas import Researcher

ENDPOINT_URL = '/researcherPatent'


@pytest.mark.asyncio
async def test_get_researcher_patent(client, create_patent):
    # Arrange
    expected_count = 2
    await create_patent()
    await create_patent()

    # Act
    response = client.get(ENDPOINT_URL)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
    assert [Researcher.Researcher(**item) for item in data]


@pytest.mark.asyncio
async def test_term_filter_on_patent_title(client, create_patent):
    # Arrange
    target_title = 'Sistema de Energia Solar Avançado'
    await create_patent(title='Outro Tipo de Invenção')
    await create_patent(title=target_title)

    expected_count = 1
    params = {'term': 'Solar'}

    # Act
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count


@pytest.mark.asyncio
async def test_graduate_program_filter(
    client, create_patent, create_graduate_program, link_researcher_to_program
):
    graduate_program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

    params = {'graduate_program': graduate_program['name']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_department_filter(
    client, create_patent, create_department, link_researcher_to_department
):
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

    params = {'departament': department['dep_nom']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_institution_filter(
    client, create_patent, create_institution, create_researcher
):
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    await create_patent()
    await create_patent(researcher_id=researcher['id'])

    params = {'institution': institution['name']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_city_filter(client, create_patent, create_researcher_production):
    city = 'Belo Horizonte'
    prod = await create_researcher_production(city=city)

    await create_patent()
    await create_patent(researcher_id=prod['researcher_id'])

    params = {'city': city}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_area_filter(client, create_patent, create_researcher_production):
    great_area_ = ['Tecnologia', 'Engenharia']
    prod = await create_researcher_production(great_area_=great_area_)

    await create_patent()
    await create_patent(researcher_id=prod['researcher_id'])

    params = {'area': ';'.join(great_area_)}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_research_group_filter_by_name(
    client,
    create_patent,
    create_research_group,
    link_researcher_to_research_group,
):
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

    params = {'group': group['name']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_modality_filter(client, create_patent, create_foment):
    foment_info = await create_foment(
        modality_name='Bolsa de Inovação Tecnológica'
    )

    await create_patent()
    await create_patent(researcher_id=foment_info['researcher_id'])

    params = {'modality': foment_info['modality_name']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_researcher_id_filter(client, create_patent):
    await create_patent()
    patent = await create_patent()

    params = {'researcher_id': patent['researcher_id']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_lattes_id_filter(client, create_patent, create_researcher):
    lattes_id = 'ABC123'
    researcher = await create_researcher(lattes_id=lattes_id)

    await create_patent()
    await create_patent(researcher_id=researcher['id'])

    params = {'lattes_id': lattes_id}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_graduation_filter(client, create_patent, create_researcher):
    graduation = 'Doutorado'
    researcher = await create_researcher(graduation=graduation)

    await create_patent()
    await create_patent(researcher_id=researcher['id'])

    params = {'graduation': graduation}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_department_id_filter(
    client, create_patent, create_department, link_researcher_to_department
):
    department = await create_department()
    linked = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

    params = {'dep_id': department['dep_id']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_graduate_program_id_filter(
    client, create_patent, create_graduate_program, link_researcher_to_program
):
    program = await create_graduate_program()
    linked = await link_researcher_to_program(
        graduate_program_id=program['graduate_program_id']
    )

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

    params = {'graduate_program_id': program['graduate_program_id']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_research_group_filter_by_id(
    client,
    create_patent,
    create_research_group,
    link_researcher_to_research_group,
):
    group = await create_research_group()
    linked = await link_researcher_to_research_group(
        research_group_id=group['id']
    )

    await create_patent()
    await create_patent(researcher_id=linked['researcher_id'])

    params = {'group_id': group['id']}
    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == 1


@pytest.mark.asyncio
async def test_filter_by_star(
    client,
    create_patent,
    create_star_entry,
    override_get_current_user,
):
    await create_patent()
    researcher = await create_patent()

    star = await create_star_entry(
        entry_id=researcher['researcher_id'],
        type='RESEARCHER',
    )

    override_get_current_user({'user_id': star['user_id']})

    expected_count = 1

    params = {'star': True, 'type': 'ARTICLE'}

    response = client.get(ENDPOINT_URL, params=params)
    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_count
