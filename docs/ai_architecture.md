# Arquitetura de IA

O SIMCC utiliza uma camada de IA desacoplada para garantir flexibilidade de modelos e facilidade de testes.

## Estrutura do Módulo `ai/`

- **`providers/`**: Interfaces abstratas (`base.py`) e implementações concretas (ex: `openai_provider.py`).
- **`prompts/`**: Armazena templates de prompts (ex: `maria_prompts.py`). Evite prompts inline nos serviços.
- **`schemas/`**: DTOs específicos para entradas e saídas de modelos de IA.
- **`dependencies.py`**: Fábricas para Injeção de Dependência no FastAPI.

## Como Implementar Novas Funcionalidades de IA

### 1. Definir o Provider
Sempre utilize as interfaces `LLMProvider` ou `EmbeddingsProvider`. Se precisar de um novo modelo (ex: Anthropic), crie uma nova classe em `providers/` herdando das bases.

### 2. Criar o Prompt
Adicione seu template em `src/simcc/ai/prompts/`. Utilize marcadores como `{context}` para interpolação.

### 3. Injetar no Service
IA **nunca** deve ser chamada diretamente nos Routers ou Repositories. Ela deve ser injetada no Service.

```python
class MyService:
    def __init__(self, llm: LLMProvider):
        self.llm = llm

    async def execute_task(self, data):
        prompt = MY_PROMPT.format(data=data)
        return await self.llm.generate(prompt)
```

### 4. Configurar a Dependência
Em `src/simcc/ai/dependencies.py`, configure como o FastAPI deve instanciar seu provedor.

## Testes (Tier A)
Ao testar serviços que usam IA, **sempre utilize Mocks**. Nunca faça chamadas reais para APIs externas (OpenAI, etc.) em ambientes de teste automatizados.

```python
llm = MagicMock(spec=LLMProvider)
llm.generate = AsyncMock(return_value="resposta mockada")
service = MyService(llm)
```
