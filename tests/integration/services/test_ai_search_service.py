import pytest
from simcc.services.ai_search_service import AISearchService


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_researchers_by_institution_filter(session, mock_embeddings_provider):
    """
    Testa a recuperação no banco de dados filtrando especificamente por instituição (sigla).
    """
    search_service = AISearchService(embeddings_provider=mock_embeddings_provider)
    
    # Busca pesquisadores da UNEB
    results = await search_service.search_researchers_hybrid(
        session=session,
        query="",
        limit=10,
        filters={"institutions": ["UNEB"]}
    )
    
    assert len(results) > 0
    # Todos os resultados retornados devem pertencer à UNEB
    for r in results:
        assert r["institution_acronym"] == "UNEB" or "Estado da Bahia" in (r["institution"] or "")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_researchers_by_name_filter(session, mock_embeddings_provider):
    """
    Testa a recuperação no banco de dados filtrando por nome do pesquisador.
    """
    search_service = AISearchService(embeddings_provider=mock_embeddings_provider)
    
    results = await search_service.search_researchers_hybrid(
        session=session,
        query="",
        limit=5,
        filters={"researcher_name": "Eduardo Manuel"}
    )
    
    assert len(results) >= 1
    assert "Eduardo Manuel De Freitas Jorge" in results[0]["name"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_researchers_multi_institution(session, mock_embeddings_provider):
    """
    Testa a recuperação no banco cruzando múltiplas instituições (ex: UFBA e UNEB).
    """
    search_service = AISearchService(embeddings_provider=mock_embeddings_provider)
    
    results = await search_service.search_researchers_hybrid(
        session=session,
        query="tecnologia",
        limit=10,
        filters={"institutions": ["UFBA", "UNEB"]}
    )
    
    assert len(results) > 0
    found_acronyms = {r.get("institution_acronym") for r in results if r.get("institution_acronym")}
    assert ("UFBA" in found_acronyms) or ("UNEB" in found_acronyms)
