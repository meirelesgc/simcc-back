# Quickstart & Validation Guide: IA (MarIA) e Cache Distribuído

Este guia descreve os cenários de teste e validação ponta a ponta para o pacote de melhorias da MarIA e do Cache Redis.

## 1. Pré-requisitos & Ambiente

- Python 3.13+ com dependências instaladas via `poetry install`.
- Instância do PostgreSQL com `pgvector` ativa (ou contêiner via `testcontainers`).
- Instância do Redis local ou via Docker:
  ```bash
  docker run -d --name simcc-redis -p 6379:6379 redis:7-alpine
  ```

## 2. Configuração de Variáveis de Ambiente (`.env`)

```ini
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/simcc
REDIS_URL=redis://localhost:6379/0
REDIS_ENABLED=true
AI_COSINE_DISTANCE_THRESHOLD=0.65
AI_CACHE_TTL=3600
# OPENAI_API_KEY=sk-... (Opcional nos testes automatizados com mocks)
```

## 3. Cenários de Validação

### Cenário 1: Execução da Suíte de Testes Automatizados (Sem OPENAI_API_KEY)
Garante que todos os testes unitários e de integração rodam com mocks sem necessidade de chave externa:
```bash
poetry run pytest tests/unit/services/test_maria_service.py -v
poetry run pytest tests/api/routers/test_maria_router.py -v
poetry run pytest tests/unit/core/test_cache_service.py -v
```
**Resultado Esperado**: Todos os testes passam com mocks, sem exceções de chave ausente.

### Cenário 2: Validação de Chave de API Ausente em Tempo de Execução
Ao realizar uma requisição para `/ai/chat/ask` sem `OPENAI_API_KEY` configurada e sem mocks:
```bash
curl -X POST http://localhost:8000/ai/chat/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "Pesquisadores da UFBA em IA"}'
```
**Resultado Esperado**: Retorno HTTP 503 com JSON amigável informando que o serviço de IA está temporariamente indisponível.

### Cenário 3: Validação do Cache Redis (Hit e Redução de Latência)
1. Enviar primeira consulta:
   ```bash
   curl -X POST http://localhost:8000/ai/chat/ask \
     -H "Content-Type: application/json" \
     -d '{"query": "Quais artigos foram publicados sobre dengue na Bahia?"}'
   ```
2. Verificar no Redis a criação da chave:
   ```bash
   redis-cli keys "simcc:ai:chat:*"
   ```
3. Reenviar a mesma consulta e verificar nos logs estruturados:
   - Evento `ai.chat.cache_hit` com `cache_hit: true`
   - Tempo de resposta inferior a 50ms.

### Cenário 4: Validação da Linha de Corte e Mensagem Empática
Enviar consulta com tema inexistente na base:
```bash
curl -X POST http://localhost:8000/ai/chat/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "Pesquisadores especializados em propulsão de naves interestelares"}'
```
**Resultado Esperado**: A MarIA responde com mensagem amigável e acolhedora avisando que os dados para esse tema ainda não foram identificados na base do SIMCC (que está em constante indexação) e sugere termos correlatos.
