from unittest.mock import AsyncMock

import pytest

from simcc.ai.query_planner import QueryPlan, SearchFilters
from simcc.services.maria_service import MariaService


@pytest.mark.unit
@pytest.mark.asyncio
async def test_chat_ask_researcher_search(
    mock_llm_provider, mock_embeddings_provider
):
    """
    Testa se o MariaService orquestra corretamente o QueryPlanner,
    o AISearchService e a geração de resposta para busca de pesquisadores.
    """
    service = MariaService(
        llm=mock_llm_provider, embeddings=mock_embeddings_provider
    )

    # Mock do Planner
    planner = AsyncMock()
    planner.plan.return_value = QueryPlan(
        intent='researcher_search',
        semantic_query='linguística',
        filters=SearchFilters(institutions=['UNEB']),
    )

    # Mock do SearchService
    search_service = AsyncMock()
    search_service.search_researchers_hybrid.return_value = [
        {
            'id': '12345',
            'name': 'Adilson Da Silva Correia',
            'institution': 'Universidade Do Estado Da Bahia',
            'institution_acronym': 'UNEB',
            'lattes_id': '9999',
            'abstract': 'Doutor em Linguística',
            'semantic_content': 'Pesquisador da UNEB em Linguística',
        }
    ]

    session = AsyncMock()

    # Executa chat_ask
    response = await service.chat_ask(
        session=session,
        query='Quais pesquisadores da UNEB trabalham com linguística?',
        planner=planner,
        search_service=search_service,
    )

    # Asserções
    assert response.intent == 'researcher_search'
    assert response.filters_extracted['institutions'] == ['UNEB']
    assert len(response.researchers) == 1
    assert response.researchers[0]['name'] == 'Adilson Da Silva Correia'
    assert 'Adilson Da Silva Correia (UNEB)' in response.sources
    assert len(response.answer) > 0

    # Verifica se o search_service foi chamado com os argumentos esperados
    search_service.search_researchers_hybrid.assert_called_once_with(
        session=session,
        query='linguística',
        limit=10,
        filters={'institutions': ['UNEB']},
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_chat_ask_no_results_handling(
    mock_llm_provider, mock_embeddings_provider
):
    """
    Testa o comportamento quando a busca não retorna nenhum resultado.
    """
    service = MariaService(
        llm=mock_llm_provider, embeddings=mock_embeddings_provider
    )

    planner = AsyncMock()
    planner.plan.return_value = QueryPlan(
        intent='researcher_search',
        semantic_query='astrofísica quântica',
        filters=SearchFilters(institutions=['INEXISTENTE']),
    )

    search_service = AsyncMock()
    search_service.search_researchers_hybrid.return_value = []

    session = AsyncMock()

    response = await service.chat_ask(
        session=session,
        query='Pesquisadores de astrofísica na instituição inexistente',
        planner=planner,
        search_service=search_service,
    )

    assert response.intent == 'researcher_search'
    assert len(response.researchers) == 0
    assert response.sources == []
