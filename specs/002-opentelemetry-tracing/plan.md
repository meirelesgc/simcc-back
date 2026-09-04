# Implementation Plan: Implementação de Rastreabilidade Distribuída e Telemetria com OpenTelemetry

**Branch**: `002-opentelemetry-tracing` | **Date**: 2026-09-03 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/002-opentelemetry-tracing/spec.md`

## Summary

Implementação incremental e vendor-neutral do OpenTelemetry no SIMCC Backend. O plano estrutura um módulo centralizado em `src/simcc/core/telemetry/`, instrumenta o ciclo de vida HTTP no FastAPI com Semantic Conventions, evolui a classe `AITracer` para emitir a árvore hierárquica de spans da MarIA (`planner`, `retrieval`, `cutoff`, `synthesis`), instrumenta operações de banco de dados e cache sem exposição de dados sensíveis, define métricas operacionais agregadas com baixa cardinalidade e mantém a coexistência transparente com o sistema de logs estruturados em JSONL já em produção.

---

## Technical Context

**Language/Version**: Python 3.13+ gerenciado via Poetry

**Primary Dependencies**:
* `opentelemetry-api` (>=1.30.0)
* `opentelemetry-sdk` (>=1.30.0)
* `opentelemetry-exporter-otlp` (>=1.30.0)
* `opentelemetry-instrumentation-fastapi` (>=0.51b0)
* `opentelemetry-instrumentation-httpx` (>=0.51b0)
* `opentelemetry-instrumentation-redis` (>=0.51b0)
* `opentelemetry-instrumentation-sqlalchemy` (>=0.51b0)
* `structlog` (>=26.1.0)
* `pydantic-settings` (>=2.14.1)

**Storage**: PostgreSQL 17 com `pgvector` e `unaccent`, Redis 7+ para cache distribuído

**Testing**: Pytest 9.0+, `pytest-asyncio`, `testcontainers` (PostgreSQL), `unittest.mock` com `InMemorySpanExporter` e `InMemoryMetricReader`

**Target Platform**: Linux Server / Docker Compose / Uvicorn (múltiplos workers)

**Project Type**: Web Service / REST API + SSE Streaming + IA

**Performance Goals**:
* Overhead computacional de telemetria < 2% na latência das requisições;
* Processamento e envio de spans desvinculado do loop síncrono do cliente (`BatchSpanProcessor`).

**Constraints**:
* Preservação estrita dos contratos de API do frontend (`/ai/chat/ask` e `/ai/chat/ask/stream`);
* Proibição expressa de expor comandos SQL com valores, textos integrais de perguntas, respostas brutas ou dados pessoais nos traces;
* Dimensões de métricas limitadas a conjuntos fixos e controlados de baixa cardinalidade;
* Preservação integral do formato e da rotina de retenção dos logs diários JSONL.

**Scale/Scope**: Múltiplos workers Uvicorn; rastreabilidade de ponta a ponta de todas as rotas e pipelines de IA.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Princípio I (Preservação de Especificações Legadas)**: PASS. A introdução do OpenTelemetry é transparente e aditiva; nenhuma rota, contrato de payload ou formato de resposta é modificado.
- **Princípio II (Isolamento em Git Worktree & Develop)**: PASS. O desenvolvimento é conduzido no escopo isolado da feature `002-opentelemetry-tracing`.
- **Princípio III (Commits Granulares em Português)**: PASS. O plano estabelece etapas atômicas (Setup, FastAPI, AI Spans, Infra, Métricas, Governança).
- **Princípio IV (Metodologia de Testes em Camadas)**: PASS. Testes unitários com `InMemorySpanExporter` para checar spans e métricas; testes de integração de API sem necessidade de Collector externo.
- **Princípio V (Observabilidade & Logging JSONL)**: PASS. O sistema existente de logs JSONL permanece intacto e é enriquecido com a injeção do `trace_id` correspondente.
- **Princípio VI (Documentação Humanizada com MkDocs)**: PASS. As diretrizes de governança de telemetria serão documentadas em `docs/observability.md` e indexadas no `mkdocs.yml`.

---

## Project Structure

### Documentation (this feature)

```text
specs/002-opentelemetry-tracing/
├── plan.md              # Este plano de implementação
├── research.md          # Decisões tecnológicas e levantamento de bibliotecas
├── data-model.md        # Entidades conceituais e definições de telemetria
├── quickstart.md        # Guia prático de execução e validação local
├── contracts/           # Contratos formais de spans e métricas
│   ├── telemetry_spans_contract.md
│   └── metrics_contract.md
├── checklists/
│   └── requirements.md
└── tasks.md             # Tarefas de implementação (geradas pelo /speckit-tasks)
```

### Source Code Layout

```text
src/simcc/
├── core/
│   ├── telemetry/               # Novo módulo de telemetria OTel
│   │   ├── __init__.py          # Ponto de entrada: init_telemetry(app)
│   │   ├── config.py            # TelemetrySettings (OTEL_ENABLED, EXPORTER, etc.)
│   │   ├── tracing.py           # TracerProvider, Resource, BatchProcessor e instrumentadores
│   │   └── metrics.py           # MeterProvider, Histogramas e Contadores agregados
│   ├── logging/
│   │   └── config.py            # Enriquecimento de logs JSONL com trace_id ativo
│   └── settings.py              # Injeção das configurações de telemetria
├── ai/
│   └── telemetry/
│       └── tracer.py            # Evolução do AITracer para criar spans OTel da MarIA
└── routers/
    └── maria.py                 # Rastreamento das rotas de chat em lote e streaming

tests/
├── unit/
│   └── telemetry/
│       ├── test_tracing_setup.py # Validação de TracerProvider e Resource
│       ├── test_ai_tracer_spans.py# Validação da árvore de spans da MarIA
│       └── test_metrics.py       # Validação de contadores e histogramas
└── integration/
    └── telemetry/
        └── test_api_tracing.py   # Testes ponta a ponta de requisições com TestClient
```

---

## Complexity Tracking

| Decisão | Por que é necessária | Alternativa mais simples rejeitada porque |
|:---|:---|:---|
| `BatchSpanProcessor` assíncrono | Evita que chamadas de rede para o OTLP/Collector penalizem a latência das respostas da API | `SimpleSpanProcessor` síncrono bloqueia a requisição a cada span emitido |
| Centralização em `simcc/core/telemetry/` | Desacopla o `main.py` e os serviços de negócio do mecanismo de transporte de telemetria | Configuração solta em múltiplos arquivos tornaria complexa a futura troca de backend visual |
| Dimensões restritas em métricas | Assegura que métricas agregadas possam ser mantidas por meses sem gerar explosão de séries temporais | Incluir `user_id` ou texto de consulta tornaria o armazenamento inviável financeiramente e tecnicamente |
