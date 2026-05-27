from abc import ABC, abstractmethod
from typing import Any, List


class LLMProvider(ABC):
    @abstractmethod
    async def generate(self, prompt: str, **kwargs: Any) -> str:
        pass


class EmbeddingsProvider(ABC):
    @abstractmethod
    async def get_embeddings(self, text: str) -> List[float]:
        pass
