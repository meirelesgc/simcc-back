from http import HTTPStatus

import pytest

from simcc.schemas import Researcher


@pytest.mark.asyncio
async def test_get_researcher_participation_event(
    client, create_participation_event
):
    EXPECTED_COUNT = 1
    for _ in range(EXPECTED_COUNT):
        await create_participation_event()
    response = client.get('/researcherParticipationEvent')

    assert response.status_code == HTTPStatus.OK
    assert len(response.json()) == EXPECTED_COUNT
    assert [Researcher.Researcher(**item) for item in response.json()]


@pytest.mark.asyncio
async def test_get_researcher_participation_event_(
    client, create_participation_event
):
    EXPECTED_COUNT = 2
    for _ in range(EXPECTED_COUNT):
        await create_participation_event()
    response = client.get('/researcherParticipationEvent')

    assert response.status_code == HTTPStatus.OK
    assert len(response.json()) == EXPECTED_COUNT
    assert [Researcher.Researcher(**item) for item in response.json()]


@pytest.mark.asyncio
async def test_graduate_program_filter(
    client,
    create_participation_event,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    graduate_program = await create_graduate_program()
    researcher = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_participation_event()
    await create_participation_event(researcher_id=researcher['researcher_id'])

    expected_events_count = 1
    params = {'graduate_program': graduate_program['name']}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_graduate_program_id_filter(
    client,
    create_participation_event,
    create_graduate_program,
    link_researcher_to_program,
):
    # Arrange
    graduate_program = await create_graduate_program()
    researcher = await link_researcher_to_program(
        graduate_program_id=graduate_program['graduate_program_id']
    )

    await create_participation_event()
    await create_participation_event(researcher_id=researcher['researcher_id'])

    expected_events_count = 1
    params = {'graduate_program_id': graduate_program['graduate_program_id']}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_researcher_id_filter(client, create_participation_event):
    # Arrange
    await create_participation_event()
    participation_event = await create_participation_event()

    expected_events_count = 1
    params = {'researcher_id': participation_event['researcher_id']}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_department_filter(
    client,
    create_participation_event,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_participation_event()
    await create_participation_event(researcher_id=link_info['researcher_id'])

    expected_events_count = 1
    params = {'departament': department['dep_nom']}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_department_id_filter(
    client,
    create_participation_event,
    create_department,
    link_researcher_to_department,
):
    # Arrange
    department = await create_department()
    link_info = await link_researcher_to_department(dep_id=department['dep_id'])

    await create_participation_event()
    await create_participation_event(researcher_id=link_info['researcher_id'])

    expected_events_count = 1
    params = {'dep_id': department['dep_id']}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_year_filter(client, create_participation_event):
    # Arrange

    await create_participation_event(year=1900)
    await create_participation_event(year=2000)

    expected_events_count = 1
    params = {'year': 2000}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_institution_filter(
    client, create_participation_event, create_institution, create_researcher
):
    # Arrange
    institution = await create_institution()
    researcher = await create_researcher(institution_id=institution['id'])

    await create_participation_event()
    await create_participation_event(researcher_id=researcher['id'])

    expected_events_count = 1
    params = {'institution': institution['name']}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_city_filter(
    client, create_participation_event, create_researcher_production
):
    # Arrange
    city = 'Uma cidade qualquer'
    production_info = await create_researcher_production(city=city)

    await create_participation_event()
    await create_participation_event(
        researcher_id=production_info['researcher_id']
    )

    expected_events_count = 1
    params = {'city': city}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_area_filter(
    client, create_participation_event, create_researcher_production
):
    # Arrange
    great_area_ = ['XPTO', 'SBPC']
    production_info = await create_researcher_production(great_area_=great_area_)
    await create_participation_event()
    await create_participation_event(
        researcher_id=production_info['researcher_id']
    )

    expected_events_count = 1
    params = {'area': ';'.join(great_area_)}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_modality_filter(
    client, create_participation_event, create_foment
):
    # Arrange
    foment_info = await create_foment(
        modality_name='Bolsa de Produtividade em Pesquisa',
    )
    await create_participation_event()
    await create_participation_event(researcher_id=foment_info['researcher_id'])

    expected_events_count = 1
    params = {'modality': foment_info['modality_name']}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_graduation_filter(
    client, create_participation_event, create_researcher
):
    # Arrange
    graduation = 'Doutorado'
    researcher = await create_researcher(graduation=graduation)
    await create_participation_event()
    await create_participation_event(researcher_id=researcher['id'])

    expected_events_count = 1
    params = {'graduation': graduation}

    # Act
    response = client.get('/researcherParticipationEvent', params=params)
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == expected_events_count


@pytest.mark.asyncio
async def test_page_filter(client, create_participation_event):
    # Arrange
    EXPECTED_COUNT = 25
    for _ in range(EXPECTED_COUNT):
        await create_participation_event()

    # Act
    response = client.get('/researcherParticipationEvent')
    data = response.json()

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert len(data) == EXPECTED_COUNT - 1
