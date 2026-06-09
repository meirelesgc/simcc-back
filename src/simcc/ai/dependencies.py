from fastapi import Depends

from simcc.ai.providers.openai_provider import OpenAIProvider
from simcc.core.dependencies import get_settings


def get_llm_provider(settings=Depends(get_settings)):
    return OpenAIProvider(api_key=settings.OPENAI_API_KEY)


def get_embeddings_provider(settings=Depends(get_settings)):
    return OpenAIProvider(api_key=settings.OPENAI_API_KEY)
