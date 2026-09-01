# Research: Melhorias na Interação com IA (MarIA) e Cache Distribuído

## 1. Camada de Cache com Redis

### Decisão
Adotar a biblioteca oficial `redis` (versão `>=5.0.0`) utilizando o módulo assíncrono `redis.asyncio` gerenciado centralmente por uma classe `CacheService` desacoplada em `src/simcc/core/cache/`.

### Rationale
- O `redis.asyncio` é nativo, assíncrono e integrado ao loop do asyncio do Python 3.13 / FastAPI.
- A aplicação roda com 4 a 6 workers Uvicorn/Gunicorn. Um pool de conexões Redis (`ConnectionPool.from_url`) compartilhado no nível do worker garante alta eficiência e sem contenção.
- O padrão `CacheService` encapsula a serialização/deserialização (JSON), namespaces de chaves (`simcc:ai:...`, `simcc:api:...`), TTLs configuráveis e política de *Graceful Fallback* (se o Redis estiver offline, a falha é capturada, logada em warning estruturado e a requisição prossegue em modo *cache bypass*).

### Alternativas Consideradas
- *Cache em memória local (ex: `@lru_cache`, `cachetools`)*: Rejeitado porque não sincroniza entre múltiplos workers (cada worker teria seu estado isolado, causando redundância de chamadas a APIs pagas e desperdício de memória).
- *aiocache*: Rejeitado por adicionar abstrações desnecessárias e dependências extras, enquanto `redis.asyncio` puro oferece controle estrito de tipos, streaming e reconexão.

---

## 2. Estratégia de Cache para Chat em Lote e Streaming (SSE)

### Decisão
- **Chave de Cache Canônica**: Hashing SHA-256 gerado a partir de parâmetros normalizados (`intent`, filtros aplicados e query semântica normalizada). Prefixo: `simcc:ai:chat:{hash}`.
- **Cache de Resposta em Lote (`/ai/chat/ask`)**: Armazena o JSON completo serializado de `ChatResponse` com TTL padrão de 3600 segundos (1 hora).
- **Cache de Streaming (`/ai/chat/ask/stream`)**: Armazena a lista ordenada de eventos do stream (`metadata`, `deltas`, `done`). Quando houver *cache hit* em requisições de stream, os eventos serializados são reproduzidos sequencialmente para o cliente, preservando a experiência visual sem refazer chamadas ao LLM.

### Rationale
- Garante paridade total entre o endpoint de lote e o endpoint de streaming.
- Elimina chamadas duplicadas aos modelos da OpenAI em pesquisas repetitivas comuns.

---

## 3. Linha de Corte de Relevância Semântica (Quality Gate)

### Decisão
Estabelecer um limiar máximo de distância cosseno no `pgvector` (`MAX_COSINE_DISTANCE = 0.65`, configurável via `Settings.AI_COSINE_DISTANCE_THRESHOLD`).

### Rationale
- Na métrica `cosine_distance` do `pgvector`, valores próximos de `0.0` indicam alta similaridade, enquanto valores acima de `0.65-0.70` representam correspondência fraca ou ruído.
- Ao descartar documentos com distância superior a `0.65`, evitamos alimentar o modelo de síntese com conteúdos irrelevantes que levam a alucinações.
- Se após a filtragem nenhum documento atender ao critério de qualidade, a pipeline ativa o cenário de resposta vazia/dados em indexação.

### Alternativas Consideradas
- *Passar todos os resultados recuperados para a IA decidir*: Rejeitado porque gera respostas confusas e desperdiça tokens em contextos inúteis.

---

## 4. Reformulação dos Prompts e Variações de Resposta

### Decisão
Reestruturar o prompt de síntese em `src/simcc/ai/prompts/maria_prompts.py` e a lógica de orquestração em `MariaService` para suportar 5 modos de resposta:
1. **Modo Volume Alto (> 5 registros)**: Panorama executivo com síntese contextual, tendências e destaque para 3 a 5 principais expoentes, orientando o usuário a refinar a busca.
2. **Modo Volume Reduzido (1 a 4 registros)**: Descrição humanizada e aprofundada de cada pesquisador ou produção encontrada.
3. **Modo Heterogêneo/Multidisciplinar**: Estruturação comparativa agrupada por instituição (ex: UFBA, UNEB, UEFS) ou tipo de produção.
4. **Modo Base em Indexação (0 registros ou pós-corte)**: Mensagem empática e transparente informando que a base do SIMCC está em constante indexação pelo Observatório SECTI e sugerindo termos alternativos.
5. **Modo Conversacional / Geral**: Saudação calorosa e explicação concisa das capacidades da MarIA sem acionar busca vetorial desnecessária.

### Rationale
- Transforma a interação fria e baseada em listagens cruas em uma experiência de consultoria científica empática e fluida.

---

## 5. Rastreabilidade e Tracing da Pipeline de IA

### Decisão
Implementar o módulo `src/simcc/ai/telemetry/tracer.py` integrado ao sistema de logs estruturados (JSONL) de `.agents/skills/logging`.

### Rationale
- Cada execução de chat gera um contexto de tracing (`AITracer`) que mede os tempos de cada etapa (`planner_ms`, `search_ms`, `cutoff_ms`, `synthesis_ms`, `cache_hit`), número de documentos filtrados e modelo utilizado.
- Emite logs na categoria `ai` com schema JSON padronizado e metadados de diagnóstico.

---

## 6. Tratamento de Ambientes com e sem OPENAI_API_KEY

### Decisão
Garantir que o sistema opere com resiliência tanto na presença quanto na ausência da `OPENAI_API_KEY`:
1. **Sem Chave (`OPENAI_API_KEY is None` ou inválida)**:
   - A inicialização da aplicação FastAPI não falha (deve subir normalmente).
   - O `OpenAIProvider` e `QueryPlanner` identificam a ausência da chave e disparam exceção tratada `AIServiceUnavailableException` ou modo fallback estruturado (HTTP 503 com mensagem amigável no endpoint REST e evento `error` no SSE).
   - A suíte de testes unitários e de integração mocka os providers e valida especificamente ambos os fluxos (chave presente e ausente).
2. **Com Chave**:
   - Operação normal com chamadas ao modelo e rastreamento de telemetria.
   - Testes de ponta a ponta reais ficam isolados sob o marcador `@pytest.mark.ai_live`.

