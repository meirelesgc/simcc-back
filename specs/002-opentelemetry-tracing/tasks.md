# Tasks: Implementação de Rastreabilidade Distribuída e Telemetria com OpenTelemetry

**Input**: Design documents from `specs/002-opentelemetry-tracing/`
**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/](./contracts/)

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Tarefa paralelizável (opera em arquivo diferente, sem dependências pendentes)
- **[Story]**: Mapeamento para as Histórias de Usuário da especificação (`[US1]` a `[US5]`)
- Todos os caminhos de arquivos são explícitos e absolutos em relação à raiz do repositório.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Instalação de dependências controladas do OpenTelemetry e configurações de ambiente

- [X] T001 Adicionar dependências oficiais do OpenTelemetry (`opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp`, `opentelemetry-instrumentation-fastapi`, `opentelemetry-instrumentation-sqlalchemy`, `opentelemetry-instrumentation-redis`, `opentelemetry-instrumentation-httpx`) no arquivo `pyproject.toml`
- [X] T002 [P] Configurar parâmetros de ambiente de telemetria (`OTEL_ENABLED`, `OTEL_EXPORTER_TYPE`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SAMPLING_RATIO`) em `src/simcc/core/settings.py`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Módulo centralizado `simcc/core/telemetry/` e infraestrutura base de exportadores

**⚠️ CRITICAL**: Nenhuma história de usuário deve ser iniciada antes da conclusão desta fase

- [X] T003 Criar modelo de configurações e validações de telemetria em `src/simcc/core/telemetry/config.py`
- [X] T004 [P] Implementar inicialização de `Resource`, `TracerProvider`, `BatchSpanProcessor` e exportadores (Console, OTLP e InMemory) em `src/simcc/core/telemetry/tracing.py`
- [X] T005 [P] Implementar inicialização de `MeterProvider` e exportadores de métricas em `src/simcc/core/telemetry/metrics.py`
- [X] T006 Implementar ponto de entrada unificado `init_telemetry(app)` e gerenciador de ciclo de vida em `src/simcc/core/telemetry/__init__.py`

**Checkpoint**: Camada base de telemetria pronta e desacoplada do código de negócio

---

## Phase 3: User Story 1 - Rastreamento Distribuído da API e Ciclo de Vida HTTP (Priority: P1) 🎯 MVP

**Goal**: Rastrear automaticamente requisições HTTP da API FastAPI com Semantic Conventions oficiais (`method`, `route`, `status_code`, `duration`)

**Independent Test**: Executar requisições em `GET /health` e verificar emissão do span raiz com `service.name=simcc-back` e atributos HTTP padronizados

### Tests for User Story 1
- [X] T007 [P] [US1] Criar testes unitários para inicialização do TracerProvider e Resource em `tests/unit/telemetry/test_tracing_setup.py`
- [X] T008 [P] [US1] Criar testes de integração para instrumentação do FastAPI e geração de spans HTTP com TestClient em `tests/integration/telemetry/test_api_tracing.py`

### Implementation for User Story 1
- [X] T009 [US1] Configurar instrumentação automática do FastAPI com filtros de rotas e normalização de caminhos em `src/simcc/core/telemetry/tracing.py`
- [X] T010 [US1] Conectar chamada `init_telemetry(app)` na inicialização da aplicação em `src/simcc/main.py`
- [X] T011 [US1] Configurar propagação de contexto W3C TraceContext nos cabeçalhos de resposta em `src/simcc/core/telemetry/tracing.py`

**Checkpoint**: Toda chamada HTTP na API produz traces e spans hierárquicos válidos (MVP alcançado)

---

## Phase 4: User Story 2 - Decomposição Semântica da Pipeline da MarIA em Spans (Priority: P1)

**Goal**: Decompor a orquestração da MarIA em uma árvore estruturada de spans (`ai.planner`, `ai.retrieval`, `ai.cutoff`, `ai.synthesis`)

**Independent Test**: Submeter consultas de chat nos modos lote e streaming, validando que cada estágio produz spans filhos vinculados ao trace da requisição

### Tests for User Story 2
- [X] T012 [P] [US2] Criar testes unitários para o ciclo de vida de spans e captura de erros no AITracer em `tests/unit/telemetry/test_ai_tracer_spans.py`
- [X] T013 [P] [US2] Criar testes de integração da rota `/ai/chat/ask` validando a árvore de spans da MarIA em `tests/integration/telemetry/test_maria_spans.py`

### Implementation for User Story 2
- [X] T014 [US2] Evoluir `AITracer` para abrir e fechar spans ativos do OpenTelemetry com atributos semânticos em `src/simcc/ai/telemetry/tracer.py`
- [X] T015 [US2] Integrar span pai `ai.pipeline` e medição de estágios no endpoint de lote em `src/simcc/services/maria_service.py`
- [X] T016 [US2] Integrar span pai `ai.pipeline` e tratamento de cancelamento no endpoint de streaming SSE em `src/simcc/services/maria_service.py`
- [X] T017 [US2] Mapear atributos de acerto de cache (`ai.cache_hit`), contagem de documentos e descarte de corte em `src/simcc/ai/telemetry/tracer.py`

**Checkpoint**: A pipeline conversacional da MarIA é completamente visível em árvore temporal de spans

---

## Phase 5: User Story 3 - Visibilidade Segura de Operações de Infraestrutura (Banco e Cache) (Priority: P2)

**Goal**: Instrumentar chamadas ao PostgreSQL (pgvector), Redis e requisições HTTP externas com estrita proteção contra vazamento de dados sensíveis

**Independent Test**: Executar requisições que acionem buscas vetoriais e leituras no cache, confirmando spans de infraestrutura sem consultas SQL parametrizadas

### Tests for User Story 3
- [X] T018 [P] [US3] Criar testes unitários para validação de sanitização de queries SQL e atributos seguros em `tests/unit/telemetry/test_db_sanitization.py`
- [X] T019 [P] [US3] Criar testes de integração para spans de comandos Redis em `tests/integration/telemetry/test_redis_tracing.py`

### Implementation for User Story 3
- [X] T020 [US3] Configurar instrumentação do SQLAlchemy com política estrita de sanitização em produção em `src/simcc/core/telemetry/tracing.py`
- [X] T021 [US3] Configurar instrumentação do cliente Redis assíncrono com supressão de chaves dinâmicas em `src/simcc/core/telemetry/tracing.py`
- [X] T022 [US3] Configurar instrumentação do cliente HTTPX para monitorar latência em chamadas à OpenAI em `src/simcc/core/telemetry/tracing.py`

**Checkpoint**: Operações com PostgreSQL, Redis e APIs externas são monitoradas com segurança de dados

---

## Phase 6: User Story 4 - Métricas Operacionais com Baixa Cardinalidade (Priority: P2)

**Goal**: Coletar métricas agregadas (histogramas de latência P50/P95/P99, contadores de taxa de erros e tokens) com dimensões estritas de baixa cardinalidade

**Independent Test**: Executar requisições concorrentes e inspecionar medidores numéricos, confirmando a ausência de IDs únicos e textos de busca como labels

### Tests for User Story 4
- [X] T023 [P] [US4] Criar testes unitários para medidores de métricas e validação de baixa cardinalidade em `tests/unit/telemetry/test_metrics_collection.py`

### Implementation for User Story 4
- [X] T024 [US4] Implementar histogramas de latência (`http.server.request.duration`, `simcc.ai.stage.duration`) em `src/simcc/core/telemetry/metrics.py`
- [X] T025 [US4] Implementar contadores operacionais (`simcc.ai.requests`, `simcc.ai.cache_hits`, `simcc.ai.tokens`, `simcc.ai.errors`) em `src/simcc/core/telemetry/metrics.py`
- [X] T026 [US4] Integrar registro de medições no término das requisições HTTP e estágios da MarIA em `src/simcc/core/telemetry/metrics.py`

**Checkpoint**: Métricas agregadas disponíveis para monitoramento histórico contínuo

---

## Phase 7: User Story 5 - Governança, Coexistência com JSONL e Preparação para o Collector (Priority: P3)

**Goal**: Injetar `trace_id` nos logs diários JSONL, suportar exportador OTLP e publicar guia normativo no MkDocs

**Independent Test**: Inspecionar linhas gravadas em `logs/*.jsonl` e confirmar o campo `trace_id` preenchido e correlacionado com o trace ativo

### Tests for User Story 5
- [X] T027 [P] [US5] Criar teste unitário para injeção do trace_id no format_schema_processor em `tests/unit/telemetry/test_log_correlation.py`

### Implementation for User Story 5
- [X] T028 [US5] Injetar `trace_id` e `span_id` no processador de logs estruturados em `src/simcc/core/logging/config.py`
- [X] T029 [US5] Implementar transporte OTLP (gRPC e HTTP) com tratamento assíncrono de falhas de conexão em `src/simcc/core/telemetry/tracing.py`
- [X] T030 [US5] Criar guia de governança e boas práticas de observabilidade em `docs/observability.md`
- [X] T031 [US5] Adicionar página de observabilidade ao índice de navegação em `mkdocs.yml`

**Checkpoint**: Coexistência plena entre logs e traces, documentação atualizada e exportação pronta para o Collector

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Validação de qualidade de código, execução dos cenários de teste e auditoria da documentação

- [X] T032 Executar checagem de tipos, lint e formatação de código (`poetry run ruff check .` e `poetry run ruff format .`)
- [X] T033 Executar validação prática dos cenários descritos em `specs/002-opentelemetry-tracing/quickstart.md`
- [X] T034 Validar compilação estrita da documentação com MkDocs (`poetry run mkdocs build --strict`)
- [X] T035 Executar suíte completa de testes da aplicação (`poetry run task test`)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: Sem dependências, inicia imediatamente.
- **Foundational (Phase 2)**: Depende da conclusão do Setup. Bloqueia todas as histórias de usuário.
- **User Stories (Phases 3 a 7)**:
  - **US1 (P1)**: Inicia imediatamente após a Fase 2 (gera o MVP).
  - **US2 (P1)**: Pode iniciar após US1, estendendo o rastreamento para a IA.
  - **US3 (P2)**: Adiciona spans de banco e Redis sobre a infraestrutura existente.
  - **US4 (P2)**: Conecta instrumentos de métricas nos hooks criados nas fases anteriores.
  - **US5 (P3)**: Amarra a correlação de logs JSONL e documentação de governança.
- **Polish (Phase 8)**: Depende da conclusão de todas as histórias.

### Parallel Opportunities

- Tarefas marcadas com `[P]` operam em módulos distintos e podem ser executadas concorrentemente.
- Os testes de cada história (`T007`, `T008`, `T012`, `T013`, `T018`, `T019`, `T023`, `T027`) podem ser implementados antes ou em paralelo com os respectivos componentes.

---

## Implementation Strategy (MVP First)

1. **Passo 1 (Fundação)**: Setup e Módulo Core (`T001` a `T006`).
2. **Passo 2 (MVP - Traces HTTP)**: Instrumentação do FastAPI e validação no console com `GET /health` (`T007` a `T011`).
3. **Passo 3 (Deep Tracing - MarIA)**: Árvore de spans da IA com `AITracer` (`T012` a `T017`).
4. **Passo 4 (Infraestrutura Segura)**: Spans de banco e cache sem vazamento de dados (`T018` a `T022`).
5. **Passo 5 (Métricas & Governança)**: Métricas com baixa cardinalidade, correlação com JSONL e MkDocs (`T023` a `T031`).
6. **Passo 6 (Quality Gate)**: Lint, testes e build da documentação (`T032` a `T035`).
