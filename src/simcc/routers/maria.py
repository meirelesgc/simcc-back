from enum import Enum

from fastapi import APIRouter, Depends, Query

from simcc.ai.dependencies import get_embeddings_provider, get_llm_provider, get_query_planner, get_ai_search_service
from simcc.ai.schemas.maria import MariaResponse, ChatRequest, ChatResponse
from simcc.core.dependencies import AsyncSession
from simcc.schemas import DefaultFilters
from simcc.services.maria_service import MariaService

router = APIRouter(tags=['MarIA - Inteligência Artificial'])


class SearchType(str, Enum):
    abstract = 'abstract'
    article = 'article'
    article_abstract = 'article_abstract'
    book = 'book'
    event = 'event'
    patent = 'patent'


def get_maria_service(
    llm=Depends(get_llm_provider), embeddings=Depends(get_embeddings_provider)
):
    return MariaService(llm, embeddings)


@router.get('/ai/researcher/summarize', response_model=MariaResponse)
async def summarize_researcher(
    session: AsyncSession,
    query: str = Query(
        ..., description='Termo ou pergunta para busca e resumo'
    ),
    search_type: SearchType = Query(
        SearchType.article, description='Onde buscar contexto'
    ),
    service: MariaService = Depends(get_maria_service),
):
    return await service.search_and_summarize(
        session, query, search_type.value
    )


@router.post('/ai/chat/ask', response_model=ChatResponse)
async def chat_ask(
    session: AsyncSession,
    request: ChatRequest,
    service: MariaService = Depends(get_maria_service),
    planner=Depends(get_query_planner),
    search_service=Depends(get_ai_search_service)
):
    """
    Interface de chat principal com a MarIA, suportando Query Planning e Busca Híbrida.
    """
    return await service.chat_ask(session, request.query, planner, search_service)


@router.get('/ai/production/classify')
async def classify_production(
    session: AsyncSession,
    production_id: str = Query(...),
    service: MariaService = Depends(get_maria_service),
):
    """
    Classifica uma produção científica usando IA.
    """
    # TODO: Implementar lógica de classificação no MariaService
    return {'message': 'Funcionalidade em desenvolvimento'}


@router.get('/ai/summary_search/', include_in_schema=False)
async def summary_search(
    session: AsyncSession,
    filters: DefaultFilters = Depends(),
    service: MariaService = Depends(get_maria_service),
):
    """
    Busca produções ou pesquisadores e gera um resumo (MarIA) em texto puro.
    Utilizado para compatibilidade com sistemas legados.
    """
    return await service.generate_search_summary(session, filters)


@router.get(
    '/maria/researcher/{search_type}',
    response_model=MariaResponse,
    include_in_schema=False,
)
async def legacy_researcher_search(
    search_type: SearchType,
    session: AsyncSession,
    query: str = Query(...),
    service: MariaService = Depends(get_maria_service),
):
    return await service.search_and_summarize(
        session, query, search_type.value
    )
