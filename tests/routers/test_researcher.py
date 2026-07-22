from http import HTTPStatus

import pytest


@pytest.mark.asyncio
async def test_search_researcher_name_empty(client):
    """
    Test 1: Fazer uma requisição sem nenhum dado no banco.
    O retorno esperado é uma lista vazia.
    """
    response = client.get('/researcherName')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == []


@pytest.mark.asyncio
async def test_search_researcher_name_with_data(
    client,
    create_researcher,
    create_researcher_production,
    create_openalex_researcher,
):
    """
    Test 2: Adicionar um pesquisador via dynamic creators e testar novamente o endpoint.
    O retorno esperado é uma lista contendo o pesquisador cadastrado.
    """
    # 1. Criar o pesquisador dinamicamente
    researcher = await create_researcher(
        name='João Silva',
        lattes_id='1234567890123456',
        lattes_10_id='L12345',
        citations='Silva, J.',
        orcid='0000-0002-1825-0097',
        graduation='Doutorado',
        classification='A1',
        stars=5,
    )

    # 2. Criar as produções científicas vinculadas a ele
    researcher_production = await create_researcher_production(
        researcher_id=researcher.id, articles=10, book=1, software=3
    )

    # 3. Criar as métricas do OpenAlex vinculadas a ele
    openalex_researcher = await create_openalex_researcher(
        researcher_id=researcher.id,
        h_index=15,
        relevance_score=85,
        cited_by_count=350,
    )

    # 4. Fazer a requisição HTTP
    response = client.get('/researcherName')
    assert response.status_code == HTTPStatus.OK

    data = response.json()
    assert len(data) == 1

    researcher_data = data[0]
    assert researcher_data['id'] == str(researcher.id)
    assert researcher_data['name'] == researcher.name
    assert researcher_data['lattes_id'] == researcher.lattes_id
    assert researcher_data['lattes_10_id'] == researcher.lattes_10_id
    assert researcher_data['orcid'] == researcher.orcid
    assert researcher_data['graduation'] == researcher.graduation
    assert researcher_data['classification'] == researcher.classification

    # Validar que a produção agregada foi mapeada corretamente
    assert researcher_data['articles'] == researcher_production.articles
    assert researcher_data['book'] == researcher_production.book
    assert researcher_data['software'] == researcher_production.software

    # Validar que as métricas do OpenAlex foram agregadas
    assert researcher_data['h_index'] == openalex_researcher.h_index
    assert (
        researcher_data['relevance_score']
        == openalex_researcher.relevance_score
    )
    assert (
        researcher_data['cited_by_count'] == openalex_researcher.cited_by_count
    )
