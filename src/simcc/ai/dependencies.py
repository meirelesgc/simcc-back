from fastapi import Depends

from simcc.ai.providers.openai_provider import OpenAIProvider
from simcc.core.dependencies import get_settings


def get_llm_provider(settings=Depends(get_settings)):
    return OpenAIProvider(api_key=settings.OPENAI_API_KEY)


def get_embeddings_provider(settings=Depends(get_settings)):
    return OpenAIProvider(api_key=settings.OPENAI_API_KEY)


def get_query_planner(settings=Depends(get_settings)):
    from simcc.ai.query_planner import QueryPlanner

    return QueryPlanner(api_key=settings.OPENAI_API_KEY)


def get_ai_search_service(
    embeddings_provider=Depends(get_embeddings_provider),
):
    from simcc.services.ai_search_service import AISearchService

    return AISearchService(embeddings_provider=embeddings_provider)
