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
