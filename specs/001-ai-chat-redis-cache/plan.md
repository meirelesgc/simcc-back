# Implementation Plan: Melhorias na Interação com IA (MarIA) e Cache Distribuído

**Branch**: `001-ai-chat-redis-cache` | **Date**: 2026-09-01 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-ai-chat-redis-cache/spec.md`

## Summary

Implementação de uma camada de cache distribuído escalável baseada em Redis com suporte assíncrono para múltiplos workers, aliada à evolução da experiência de chat da MarIA. Inclui reformulação do prompt com 5 variações dinâmicas de resposta, linha de corte semântica de qualidade (`cosine_distance <= 0.65`) com mensagem empática para ausência de dados, módulo completo de telemetria/tracing em JSONL e tratamento robusto de ambientes com e sem `OPENAI_API_KEY`, mantendo estrita compatibilidade retroativa com os contratos do frontend.

## Technical Context

**Language/Version**: Python 3.13+

**Primary Dependencies**: FastAPI 0.136+, Pydantic v2 / Pydantic Settings, SQLAlchemy 2.0 (Async), `redis>=5.0.0` (`redis.asyncio`), `langchain-openai`, `structlog`

**Storage**: PostgreSQL 17 com `pgvector` e `unaccent`, Redis 7+ para cache distribuído

**Testing**: Pytest 9.0+, `pytest-asyncio`, `testcontainers` (PostgreSQL), `factory-boy`, `respx`, `unittest.mock`

**Target Platform**: Linux Server / Docker Compose / Múltiplos workers Uvicorn

**Project Type**: Web Service / REST API + SSE Streaming + IA

**Performance Goals**: Latência < 50ms para consultas repetidas em cache; redução de > 40% no consumo de tokens em queries recorrentes

**Constraints**: Preservação estrita dos contratos de API do frontend; isolamento de DB com Savepoints/Nested Transactions nos testes; conformidade rigorosa com schema JSONL de logs

**Scale/Scope**: Suporte a 4-6 workers concorrentes; consultas semânticas sobre a base de pesquisadores e produções científicas da Bahia

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Princípio I (Preservação de Especificações Legadas)**: PASS. Contratos existentes (`/ai/chat/ask`, `/ai/chat/ask/stream`, `ChatResponse`, `ChatStreamEvent`, `SearchUIMetadata`) 100% preservados.
- **Princípio II (Isolamento em Git Worktree & Develop)**: PASS. O desenvolvimento será executado em worktree isolada da branch `001-ai-chat-redis-cache` antes do merge em `develop`.
- **Princípio III (Commits Granulares em Português)**: PASS. Commits atômicos e descritivos em português para cada etapa da implementação.
- **Princípio IV (Metodologia de Testes em Camadas)**: PASS. Testes unitários com mocks para services, testes de integração de repositório e router com Savepoints, e testes específicos para cenários com e sem `OPENAI_API_KEY`.
- **Princípio V (Observabilidade & Logging JSONL)**: PASS. Módulo de telemetria emitindo eventos estruturados na categoria `ai` em conformidade com `.agents/skills/logging`.
- **Princípio VI (Documentação Humanizada com MkDocs)**: PASS. Guias conceituais e arquiteturais atualizados em `docs/` e indexados no `mkdocs.yml`.

## Project Structure

### Documentation (this feature)

```text
specs/001-ai-chat-redis-cache/
├── plan.md              # Este plano de implementação
├── research.md          # Decisões arquiteturais e de tecnologias
├── data-model.md        # Modelos conceituais e DTOs
├── quickstart.md        # Guia prático de validação e testes
├── contracts/           # Contratos de API e Cache
│   ├── ai_chat_contract.md
│   └── redis_cache_contract.md
├── checklists/
│   └── requirements.md
└── tasks.md             # Tarefas de implementação (geradas no próximo passo)
```

### Source Code Layout

```text
src/simcc/
├── ai/
│   ├── dependencies.py          # Provedores de LLM, Embeddings, Cache e Tracer
│   ├── query_planner.py         # Planejamento estruturado de consultas
│   ├── prompts/
│   │   └── maria_prompts.py     # Prompts humanizados com variações de resposta
│   ├── providers/
│   │   ├── base.py              # Interfaces abstratas
│   │   └── openai_provider.py   # Implementação com validação de API Key
│   ├── schemas/
│   │   └── maria.py             # DTOs de entrada e saída (preservados)
│   └── telemetry/
│       ├── __init__.py
│       ├── pricing.py           # Estimativa de custos e tokens
│       └── tracer.py            # Tracing e telemetria estruturada da pipeline
├── core/
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── redis_client.py      # Conexão assíncrona e pool com Redis
│   │   └── cache_service.py     # Gerenciamento de chaves, TTLs e fallback
│   ├── settings.py              # Configurações de REDIS_URL, TTLs e limiar de similaridade
│   └── logging/                 # Logging estruturado em JSONL
├── routers/
│   └── maria.py                 # Endpoints REST e Streaming com injeção de cache/tracer
└── services/
    ├── ai_search_service.py     # Busca híbrida com aplicação da linha de corte (cutoff)
    └── maria_service.py         # Orquestração da MarIA, variações de resposta e cache

tests/
├── ai/                          # Testes específicos de prompts e planners
├── api/
│   └── routers/
│       └── test_maria_router.py # Testes de integração de endpoints (com e sem API Key, cache hit/miss)
└── unit/
    ├── core/
    │   └── test_cache_service.py# Testes unitários da camada de cache
    └── services/
        └── test_maria_service.py# Testes unitários das variações da MarIA e linha de corte
```

## Complexity Tracking

| Decisão | Por que é necessária | Alternativa mais simples rejeitada porque |
|---|---|---|
| Cache com Redis Assíncrono | Suportar 4 a 6 workers Uvicorn sem contenção e sem chamadas redundantes a APIs externas | Cache em memória (`lru_cache`) não sincroniza entre múltiplos processos de workers |
| Replay de Stream via Cache | Permitir que o endpoint SSE aproveite o cache instantaneamente sem reprocessar tokens | Cachear apenas no endpoint de lote forçaria o frontend de stream a sempre pagar o custo integral da IA |
| Limiar de Distância Cosseno (`0.65`) | Filtrar ruído vetorial antes do prompt de síntese para evitar alucinações | Deixar a IA avaliar dados irrelevantes gasta tokens e confunde o usuário final |
