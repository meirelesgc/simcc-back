from typing import Any, List

from langchain_openai import ChatOpenAI, OpenAIEmbeddings

from simcc.ai.providers.base import EmbeddingsProvider, LLMProvider


class OpenAIProvider(LLMProvider, EmbeddingsProvider):
    # TODO: VOLTAR AQUI E TROCAR O MODELO PRA ALGO MAIS APROPRIADO
    def __init__(self, api_key: str, model: str = 'gpt-3.5-turbo'):
        self.llm = ChatOpenAI(api_key=api_key, model=model, temperature=0.9)
        self.embeddings = OpenAIEmbeddings(
            api_key=api_key, model='text-embedding-3-large'
        )

    async def generate(self, prompt: str, **kwargs: Any) -> str:
        response = await self.llm.ainvoke(prompt)
        return str(response.content)

    async def get_embeddings(self, text: str) -> List[float]:
        return await self.embeddings.aembed_query(text)
