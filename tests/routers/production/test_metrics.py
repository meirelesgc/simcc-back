from http import HTTPStatus

import pytest

from tests.factories import create_researcher_with_full_graph


@pytest.mark.asyncio
async def test_get_academic_degree_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/academic-degree/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_great_area_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/great-area/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_magazine_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/magazine/chart')
    assert response.status_code == HTTPStatus.OK


@pytest.mark.asyncio
async def test_get_researcher_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/researcher/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_brand_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/brand/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_research_report_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/research-report/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_events_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/events/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_papers_magazine_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/papers-magazine/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_book_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/book/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_book_chapter_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/book-chapter/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_article_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/article/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_patent_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/patent/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_guidance_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/guidance/chart')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert isinstance(data, list)
    if len(data) > 0:
        assert 'year' in data[0]
        assert 'm_completed' in data[0]
        assert 'ic_in_progress' in data[0]


@pytest.mark.asyncio
async def test_get_education_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/education/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_software_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/software/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_research_project_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/research-project/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_speaker_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/speaker/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_lattes_update_metrics(client, session):
    await create_researcher_with_full_graph(session)
    response = client.get('/metrics/lattes-update/chart')
    assert response.status_code == HTTPStatus.OK
    assert isinstance(response.json(), list)
    assert len(response.json()) > 0
    assert "total" in response.json()[0]
    assert "over_3_months" in response.json()[0]
    assert "over_6_months" in response.json()[0]
