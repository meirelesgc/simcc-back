# Tasks: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Feature**: `002-db-cleanup-legacy-schemas`
**Plan**: [plan.md](./plan.md) | **Spec**: [spec.md](./spec.md)

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Preparação do ambiente de trabalho e isolamento conforme Princípio VIII da Constituição

- [x] T001 Criar git worktree dedicado para a spec em `../simcc-back-002-db-cleanup` a partir de `develop` per Princípio VIII

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Desacoplamento de modelos legados e criação do novo modelo base `ResearcherInstitutionData`

**⚠️ CRITICAL**: Nenhuma tarefa de user story pode começar antes da conclusão desta fase

- [x] T002 Remover modelos legados (`Admin*`, `Ufmg*`) de `src/simcc/core/db/models/__init__.py` e desvincular do `table_registry`
- [x] T003 [P] Criar novo modelo SQLAlchemy `ResearcherInstitutionData` em `src/simcc/core/db/models/researcher_institution.py` com FK `researcher_id` (CASCADE, UNIQUE), colunas `zip_code`, `work_regime` e `custom_attributes` (JSONB)
- [x] T004 Registrar `ResearcherInstitutionData` em `src/simcc/core/db/models/__init__.py`
- [x] T005 [P] Atualizar modelo `Researcher` em `src/simcc/core/db/models/researcher.py` com `lattes_id: Mapped[str]` (não-nulo)

**Checkpoint**: Modelos fundamentais prontos — implementação das user stories desbloqueada

---

## Phase 3: User Story 1 - Remover Schemas Legados sem Quebrar o Frontend (Priority: P1) 🎯 MVP

**Goal**: Garantir que consultas aos schemas legados (`ufmg`, `admin`) retornem dados neutros (`[]`, `null`, `0`) sem lançar exceções para o frontend

**Independent Test**: Subir a aplicação sem os schemas legados no PostgreSQL e verificar que `/researchers` e `/researcher/departament-rt` retornam `200 OK` com dados compatíveis

### Tests for User Story 1
- [x] T006 [P] [US1] Criar testes unitários Tier A em `tests/unit/test_legacy_fallbacks.py` para validar fallbacks de schemas inexistentes (`departments=[]`, `ufmg=None`, `user=None`, `departament_rt={"teachers": 0, "technicians": 0}`)

### Implementation for User Story 1
- [x] T007 [US1] Implementar fallback resiliente em `researcher_repo.list_ufmg_data_by_ids` e `researcher_repo.list_user_data_by_lattes_ids` em `src/simcc/repositories/researcher_repo.py`
- [x] T008 [US1] Implementar fallback em `researcher_repo.get_departament_rt` em `src/simcc/repositories/researcher_repo.py` retornando contagens zeradas quando a tabela não existir
- [x] T009 [US1] Ajustar `enrich_researchers` em `src/simcc/services/researcher_service.py` para garantir retorno seguro de `departments`, `ufmg` e `user`
- [x] T010 [US1] Ajustar referências a schemas legados em `src/simcc/repositories/external_repo.py` e `src/simcc/repositories/powerBi_repo.py` para evitar exceções duras

**Checkpoint**: User Story 1 completa e testável de forma 100% independente (MVP entregue)

---

## Phase 4: User Story 3 - Integridade Referencial: `lattes_id` Obrigatório (Priority: P3)

**Goal**: Aplicar com segurança a constraint `NOT NULL` no campo `lattes_id` da tabela `researcher` após remoção prévia de dados órfãos

**Independent Test**: Executar a migração Alembic e validar que a inserção de `researcher` com `lattes_id = NULL` é rejeitada pelo banco de dados

### Tests for User Story 3
- [x] T011 [P] [US3] Criar teste de integração Tier B em `tests/integration/test_researcher_migration.py` validando que a migração limpa registros sem lattes_id e aplica a constraint `NOT NULL`

### Implementation for User Story 3
- [x] T012 [US3] Criar migração Alembic em `migrations/versions/` executando `DELETE FROM researcher WHERE lattes_id IS NULL` com logging e `ALTER TABLE researcher ALTER COLUMN lattes_id SET NOT NULL`
- [x] T013 [US3] Adicionar na migração Alembic a criação da tabela `researcher_institution_data` com constraint UNIQUE e índice em `researcher_id`
- [x] T014 [US3] Implementar função `downgrade()` segura na migração Alembic revertendo as alterações DDL

**Checkpoint**: Schema atualizado e integridade do `lattes_id` garantida no banco

---

## Phase 5: User Story 2 - Dados Proprietários e Flexíveis por Instituição (Priority: P2)

**Goal**: Disponibilizar o campo `custom_attributes` consolidado (`zip_code`, `work_regime` + JSONB) nas respostas de pesquisadores

**Independent Test**: Inserir registro em `researcher_institution_data` e verificar retorno de `custom_attributes` em `/researchers` e `/researcher/{id}`

### Tests for User Story 2
- [x] T015 [P] [US2] Criar testes unitários Tier A em `tests/unit/test_custom_attributes_enrichment.py` validando o enriquecimento de pesquisadores com e sem dados institucionais

### Implementation for User Story 2
- [x] T016 [P] [US2] Implementar função `list_institution_data_by_researcher_ids` em `src/simcc/repositories/researcher_repo.py` para consulta em lote
- [x] T017 [US2] Integrar consulta de dados institucionais na função `enrich_researchers` em `src/simcc/services/researcher_service.py` populando o campo `custom_attributes`
- [x] T018 [P] [US2] Atualizar schemas Pydantic de resposta em `src/simcc/schemas/common.py` e `src/simcc/schemas/researcher.py` declarando o campo `custom_attributes: Optional[Dict[str, Any]] = None`

**Checkpoint**: Dados flexíveis institucionais expostos corretamente na API

---

## Phase 6: User Story 4 - Script de Ingestão de Dados Proprietários via CSV (Priority: P2)

**Goal**: Fornecer script CLI reutilizável para carga de CSVs institucionais unificados na tabela `researcher_institution_data`

**Independent Test**: Executar o script com `storage/researchers/ufrb.csv` e verificar registros criados/atualizados por `researcher_id` com descarte de `name`

### Tests for User Story 4
- [x] T019 [P] [US4] Criar teste de integração Tier B em `tests/integration/test_institution_ingest.py` validando parsing, descarte de `name`, resolução por `researcher_id` e idempotência do upsert

### Implementation for User Story 4
- [x] T020 [US4] Criar script CLI `scripts/ingest/ingest_institution_researchers.py` com suporte a `--file`, resolução de `researcher_id` via `lattes_id`, separação de `zip_code` e `work_regime` e empacotamento de atributos extras em JSONB
- [x] T021 [US4] Implementar query de upsert (`INSERT ... ON CONFLICT (researcher_id) DO UPDATE`) em `scripts/ingest/ingest_institution_researchers.py`
- [x] T022 [US4] Implementar relatório de métricas na saída padrão do script com contadores de linhas processadas, gravadas e ignoradas

**Checkpoint**: Ingestão de CSV funcional, validada e idempotente para a UFRB e futuras universidades

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Verificação de qualidade, testes finais e conformidade com a Constituição

- [x] T023 [P] Executar lint e formatação com `poetry run ruff check . --fix && poetry run ruff format .` garantindo 79 colunas e aspas simples per Princípio VI
- [x] T024 Executar suíte completa de testes automatizados com `poetry run pytest -m "not ai_live"` per Princípio VII
- [x] T025 Executar validação de ponta a ponta seguindo o roteiro de [`quickstart.md`](./quickstart.md)
- [x] T026 Realizar commits atômicos em português per Princípio IX e preparar merge para `develop`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: Inicia imediatamente
- **Foundational (Phase 2)**: Depende de Setup — **BLOQUEIA** todas as user stories
- **User Story 1 (Phase 3)**: Depende de Foundational (Phase 2) — Pode rodar em paralelo com US3/US2/US4
- **User Story 3 (Phase 4)**: Depende de Foundational (Phase 2) — Cria a tabela física e aplica a constraint
- **User Story 2 (Phase 5)**: Depende de Foundational (Phase 2) e US3 (para existência da tabela física)
- **User Story 4 (Phase 6)**: Depende de Foundational (Phase 2) e US3 (para existência da tabela física)
- **Polish (Phase 7)**: Depende da conclusão de todas as User Stories

### Parallel Opportunities

- **Fase 2**: T003, T005 podem rodar em paralelo.
- **Fase 3**: T006 pode rodar em paralelo com T007.
- **Fase 4**: T011 pode rodar em paralelo com T012.
- **Fase 5**: T015, T016, T018 podem rodar em paralelo.
- **Fase 6**: T019 pode rodar em paralelo com T020.
- **Fase 7**: T023 pode rodar em paralelo com documentação.

---

## Implementation Strategy

### MVP First (User Story 1 Only)
1. Completar Setup (T001) e Foundational (T002–T005).
2. Implementar User Story 1 (T006–T010) para eliminar dependências duras de schemas legados.
3. **VALIDAR**: Verificar que a API responde com `200 OK` mesmo sem os schemas `ufmg` e `admin`.

### Entrega Incremental
1. Aplicar Migração e `lattes_id NOT NULL` (US3: T011–T014).
2. Adicionar Enriquecimento de Dados Institucionais (US2: T015–T018).
3. Criar Script de Ingestão de CSV da UFRB (US4: T019–T022).
4. Executar Polish e testes finais (T023–T026).
