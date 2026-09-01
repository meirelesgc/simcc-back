# Tasks: Melhorias na Interação com IA (MarIA) e Cache Distribuído

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Preparação de dependências, configurações e infraestrutura de cache

- [X] T001 Adicionar dependência `redis` no arquivo `pyproject.toml`
- [X] T002 [P] Adicionar serviço Redis no arquivo `compose.yaml` com mapeamento de porta e healthcheck
- [X] T003 [P] Atualizar variáveis de ambiente e configurações de Redis e IA em `src/simcc/core/settings.py`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Infraestrutura base de cache e telemetria que suportará todas as histórias de usuário

**⚠️ CRITICAL**: Nenhuma história de usuário deve ser iniciada antes da conclusão desta fase

- [X] T004 Implementar gerenciador assíncrono de conexão Redis em `src/simcc/core/cache/redis_client.py`
- [X] T005 Implementar serviço genérico de cache com namespaces, TTL e graceful fallback em `src/simcc/core/cache/cache_service.py`
- [X] T006 [P] Implementar módulo de telemetria e rastreamento de estágios de IA em `src/simcc/ai/telemetry/tracer.py`
- [X] T007 [P] Implementar estimador de custos e contagem de tokens em `src/simcc/ai/telemetry/pricing.py`
- [X] T008 Configurar injeção de dependências do CacheService e AITracer em `src/simcc/ai/dependencies.py`

**Checkpoint**: Camada base de cache e telemetria concluída e pronta para integração

---

## Phase 3: User Story 1 - Respostas Humanizadas e Adaptativas da MarIA (Priority: P1) 🎯 MVP

**Goal**: Permitir que a MarIA responda com tom empático, humanizado e com variações dinâmicas de estilo conforme a quantidade e natureza dos resultados

**Independent Test**: Enviar requisições de diferentes volumes (muitos resultados, poucos resultados, heterogêneos e vazios) e validar as respostas sintetizadas

### Tests for User Story 1
- [X] T009 [P] [US1] Criar testes unitários para templates de prompt da MarIA em `tests/unit/ai/test_maria_prompts.py`
- [X] T010 [P] [US1] Criar testes unitários para a orquestração de variações de resposta em `tests/unit/services/test_maria_service.py`

### Implementation for User Story 1
- [X] T011 [US1] Reformular templates de prompt com 5 variações comportamentais em `src/simcc/ai/prompts/maria_prompts.py`
- [X] T012 [US1] Atualizar montagem de prompt de síntese dinâmico e humanizado em `src/simcc/services/maria_service.py`
- [X] T013 [US1] Integrar emissão de deltas humanizados no fluxo de streaming SSE em `src/simcc/services/maria_service.py`

**Checkpoint**: A MarIA responde de forma amigável com 5 variações dinâmicas sem listagens mecânicas

---

## Phase 4: User Story 2 - Linha de Corte de Relevância Semântica e Tratamento de Gaps (Priority: P1)

**Goal**: Filtrar ruídos vetoriais com limiar de distância cosseno e acionar mensagem acolhedora sobre base em indexação

**Independent Test**: Submeter consultas com temas inexistentes e verificar o descarte dos documentos irrelevantes e a emissão do aviso padrão

### Tests for User Story 2
- [X] T014 [P] [US2] Criar testes unitários da linha de corte e triagem de similaridade em `tests/unit/services/test_ai_search_cutoff.py`
- [X] T015 [P] [US2] Criar testes de integração para consultas sem correspondência em `tests/api/routers/test_maria_cutoff.py`

### Implementation for User Story 2
- [X] T016 [US2] Implementar aplicação do limiar de distância cosseno (`cosine_distance <= threshold`) em `src/simcc/services/ai_search_service.py`
- [X] T017 [US2] Implementar detecção de resultados vazios/pós-corte com gatilho de resposta empática em `src/simcc/services/maria_service.py`

**Checkpoint**: Consultas irrelevantes são filtradas com precisão e respondidas com aviso orientador

---

## Phase 5: User Story 3 - Cache de Alto Desempenho com Redis (Priority: P2)

**Goal**: Armazenar em cache respostas de consultas em lote e de streaming para múltiplos workers

**Independent Test**: Executar requisições idênticas consecutivas em `/ai/chat/ask` e `/ai/chat/ask/stream` e validar tempo de resposta imediato e reemissão de eventos SSE

### Tests for User Story 3
- [X] T018 [P] [US3] Criar testes unitários para cache de respostas em lote e streaming em `tests/unit/core/test_cache_service.py`
- [X] T019 [P] [US3] Criar testes de integração do router da MarIA com cache hit e miss em `tests/api/routers/test_maria_cache.py`

### Implementation for User Story 3
- [X] T020 [US3] Implementar geração de chave canônica normalizada em `src/simcc/services/maria_service.py`
- [X] T021 [US3] Implementar verificação e gravação de cache para respostas em lote (`/ai/chat/ask`) em `src/simcc/services/maria_service.py`
- [X] T022 [US3] Implementar captura, gravação e replay de eventos SSE para streaming (`/ai/chat/ask/stream`) em `src/simcc/services/maria_service.py`

**Checkpoint**: Respostas em lote e fluxos de streaming são servidos via Redis com alta performance

---

## Phase 6: User Story 4 - Rastreabilidade e Tracing Contínuo da Pipeline de IA (Priority: P2)

**Goal**: Registrar métricas detalhadas de latência por estágio (`planner`, `search`, `cutoff`, `synthesis`) e status em logs JSONL

**Independent Test**: Executar uma consulta e verificar os logs estruturados emitidos com campos de diagnóstico da pipeline

### Tests for User Story 4
- [X] T023 [P] [US4] Criar testes unitários para coleta de métricas do AITracer em `tests/unit/ai/test_telemetry.py`

### Implementation for User Story 4
- [X] T024 [US4] Integrar medição de tempos por estágio do AITracer em `src/simcc/services/maria_service.py`
- [X] T025 [US4] Emitir logs estruturados JSONL na categoria `ai` em `src/simcc/ai/telemetry/tracer.py`

**Checkpoint**: Toda interação de chat emite métricas e logs estruturados de observabilidade

---

## Phase 7: User Story 5 - Preservação Estrita de Contratos de API e Validação com/sem OPENAI_API_KEY (Priority: P3)

**Goal**: Garantir 100% de compatibilidade retroativa com o frontend e resiliência graciosa quando `OPENAI_API_KEY` não estiver configurada

**Independent Test**: Rodar suíte de testes de integração com e sem mock de API Key e validar contratos de resposta

### Tests for User Story 5
- [X] T026 [P] [US5] Criar testes de contrato para validação de schemas do frontend em `tests/api/routers/test_maria_contracts.py`
- [X] T027 [P] [US5] Criar testes de resiliência e HTTP 503 quando OPENAI_API_KEY for None em `tests/api/routers/test_maria_no_key.py`

### Implementation for User Story 5
- [X] T028 [US5] Implementar validação de chave de API e tratamento de indisponibilidade em `src/simcc/ai/providers/openai_provider.py`
- [X] T029 [US5] Ajustar manipulador de erro amigável nos endpoints em `src/simcc/routers/maria.py`

**Checkpoint**: Schemas mantidos intactos e aplicação resiliente a ausência de credenciais

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Documentação humanizada no MkDocs, validações de qualidade e estilo

- [X] T030 [P] Criar documentação humanizada da arquitetura da MarIA e Cache em `docs/ai_architecture.md`
- [X] T031 [P] Atualizar índice de navegação em `mkdocs.yml`
- [X] T032 Executar checagem e formatação com Ruff (`poetry run ruff check .` e `poetry run ruff format .`)
- [X] T033 Executar validação de cenários de ponta a ponta descritos em `specs/001-ai-chat-redis-cache/quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies
- **Setup (Phase 1)**: Sem dependências, inicia imediatamente.
- **Foundational (Phase 2)**: Depende da conclusão do Setup (Phase 1). Bloqueia as histórias de usuário.
- **User Stories (Phases 3 a 7)**: Dependem da conclusão da Fase Foundational.
  - US1 (P1) e US2 (P1) podem avançar sequencialmente ou em paralelo.
  - US3 (P2) e US4 (P2) integram sobre o serviço da MarIA.
  - US5 (P3) valida a totalidade dos contratos e compatibilidade.
- **Polish (Phase 8)**: Depende da conclusão de todas as histórias.

### Parallel Opportunities
- Tarefas marcadas com `[P]` operam em arquivos distintos e podem ser executadas concorrentemente.
- Os testes unitários de cada fase podem ser escritos antes ou em paralelo com os modelos/provedores correspondentes.

---

## Implementation Strategy (MVP First)

1. **Passo 1 (Fundação)**: Executar Setup (T001-T003) e Foundational (T004-T008).
2. **Passo 2 (MVP - US1 + US2)**: Implementar prompts amigáveis e linha de corte de relevância (T009-T017).
3. **Passo 3 (Performance & Tracing - US3 + US4)**: Integrar cache Redis assíncrono e telemetria estruturada (T018-T025).
4. **Passo 4 (Resiliência & Contratos - US5)**: Garantir compatibilidade e fallback sem chave OpenAI (T026-T029).
5. **Passo 5 (Documentação & Qualidade)**: Atualizar MkDocs e rodar suíte de testes completa (T030-T033).
