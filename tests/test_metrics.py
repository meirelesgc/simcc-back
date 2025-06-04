from uuid import uuid4

import pytest

query_string = {
    'term': 'example_term',
    'researcher_id': str(uuid4()),
    'graduate_program_id': str(uuid4()),
    'dep_id': 'DEP001',
    'departament': 'Departamento de Exemplo',
    'year': 2023,
    'type': 'ARTICLE',
    'distinct': 1,
    'institution': 'UFABC',
    'graduate_program': 'Programa Exemplo',
    'city': 'Santo André',
    'area': 'Ciência da Computação',
    'modality': 'Presencial',
    'graduation': 'Mestrado',
}

endpoints = [
    '/researcher_metrics',
    '/brand_metrics',
    '/speaker_metrics',
    '/research_report_metrics',
    '/events_metrics',
    '/papers_magazine_metrics',
    '/book_metrics',
    '/book_chapter_metrics',
    '/article_metrics',
    '/patent_metrics',
    '/guidance_metrics',
    '/academic_degree_metrics',
    '/software_metrics',
    '/research_project_metrics',
]


@pytest.mark.parametrize('endpoint', endpoints)
def test_metrics_endpoint(endpoint, client):
    extra_params = {}
    if endpoint == '/brand_metrics':
        extra_params['nature'] = 'Exemplo'
    if endpoint == '/article_metrics':
        extra_params['qualis'] = 'A1'

    params = {**query_string, **extra_params}
    response = client.get(endpoint, params=params)
    assert response.status_code == 200
