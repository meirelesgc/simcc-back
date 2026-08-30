from http import HTTPStatus

import pytest


@pytest.mark.ai_live
def test_live_uneb_linguistics_search(live_client):
    """
    Pergunta 1: "Quais pesquisadores da UNEB trabalham com linguística ou ensino de línguas?"
    Valida: Filtro por UNEB + busca semântica + retorno de Adilson e/ou Abinalio.
    """
    # Arrange
    payload = {
        'query': 'Quais pesquisadores da UNEB trabalham com linguística ou ensino de línguas?'
    }

    # Act
    response = live_client.post('/ai/chat/ask', json=payload)

    # Assert
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert 'UNEB' in data['filters_extracted'].get('institutions', [])
    names = [r['name'] for r in data['researchers']]
    assert any('Adilson' in n or 'Abinalio' in n for n in names)
    assert len(data['answer']) > 50


@pytest.mark.ai_live
def test_live_ai_broad_search(live_client):
    """
    Pergunta 2: "Quais pesquisadores da Bahia trabalham com inteligência artificial, ciência de dados ou tecnologias digitais?"
    Valida: Busca ampla sem filtro fixo de instituição, recuperando Eduardo Manuel.
    """
    # Arrange
    payload = {
        'query': 'Quais pesquisadores da Bahia trabalham com inteligência artificial, ciência de dados ou tecnologias digitais?'
    }

    # Act
    response = live_client.post('/ai/chat/ask', json=payload)

    # Assert
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    names = [r['name'] for r in data['researchers']]
    assert any('Eduardo Manuel' in n for n in names)


@pytest.mark.ai_live
def test_live_researcher_profile_lookup(live_client):
    """
    Pergunta 3: "Quem é Eduardo Manuel de Freitas Jorge e quais são suas principais áreas de atuação?"
    Valida: intent 'researcher_profile', extração de nome e síntese biográfica.
    """
    # Arrange
    payload = {
        'query': 'Quem é Eduardo Manuel de Freitas Jorge e quais são suas principais áreas de atuação?'
    }

    # Act
    response = live_client.post('/ai/chat/ask', json=payload)

    # Assert
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['intent'] == 'researcher_profile'
    assert len(data['researchers']) >= 1
    assert 'Eduardo Manuel De Freitas Jorge' in data['researchers'][0]['name']
    assert (
        'UNEB' in data['answer']
        or 'Ciência' in data['answer']
        or 'Inteligência' in data['answer']
    )


@pytest.mark.ai_live
def test_live_institution_comparison(live_client):
    """
    Pergunta 5: "Compare os pesquisadores da UFBA e da UNEB que trabalham com tecnologia e inovação."
    Valida: extração de ['UFBA', 'UNEB'] e agrupamento/comparação na síntese.
    """
    # Arrange
    payload = {
        'query': 'Compare os pesquisadores da UFBA e da UNEB que trabalham com tecnologia e inovação.'
    }

    # Act
    response = live_client.post('/ai/chat/ask', json=payload)

    # Assert
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    institutions = data['filters_extracted'].get('institutions', [])
    assert 'UFBA' in institutions or 'UNEB' in institutions
    assert len(data['researchers']) >= 2
