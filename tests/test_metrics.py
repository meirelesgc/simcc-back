from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient

# Todos os endpoints
endpoints = [
    '/researcher_metrics',
    '/brand_metrics',
    '/speaker_metrics',
    '/research_report_metrics',
    '/papers_magazine_metrics',
    '/book_metrics',
    '/book_chapter_metrics',
    '/article_metrics',
    '/patent_metrics',
    '/guidance_metrics',
    '/academic_degree_metrics',
    '/academic_degree',
    '/software_metrics',
    '/research_project_metrics',
]


# Testa todos os endpoints sem parâmetros
@pytest.mark.asyncio
@pytest.mark.parametrize('endpoint', endpoints)
async def test_endpoint_no_params(client: TestClient, endpoint: str):
    response = client.get(endpoint)
    assert response.status_code == HTTPStatus.OK


# Testa todos os endpoints com parâmetros simples
@pytest.mark.asyncio
@pytest.mark.parametrize('endpoint', endpoints)
async def test_endpoint_with_params(client: TestClient, endpoint: str):
    # Parâmetros comuns
    params = {
        'term': 'ciência',
        'year': 2022,
        'type': 'ARTICLE',
        'distinct': 1,
        'modality': 'Presencial',
    }

    # Se for /researcher_metrics, incluir type só se tiver term
    if endpoint == '/researcher_metrics':
        response = client.get(endpoint, params=params)
    else:
        # Também testa o endpoint sem "term", mas com "type" se não for researcher_metrics
        alt_params = params.copy()
        alt_params.pop('term')
        response = client.get(endpoint, params=alt_params)

    assert response.status_code == HTTPStatus.OK


# Testa os endpoints que exigem parâmetros adicionais
@pytest.mark.asyncio
@pytest.mark.parametrize('endpoint', ['/brand_metrics', '/article_metrics'])
async def test_endpoint_with_special_params(client: TestClient, endpoint: str):
    params = {
        'term': 'tecnologia',
        'year': 2023,
        'type': 'BOOK',
    }

    if endpoint == '/brand_metrics':
        params['nature'] = 'some-nature'
    elif endpoint == '/article_metrics':
        params['qualis'] = 'A1'

    response = client.get(endpoint, params=params)
    assert response.status_code == HTTPStatus.OK
