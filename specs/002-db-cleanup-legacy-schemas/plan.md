# Implementation Plan: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Branch**: `002-db-cleanup-legacy-schemas` | **Date**: 2026-08-29 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-db-cleanup-legacy-schemas/spec.md`

## Summary

Esta feature desacopla a aplicação dos schemas legados (`ufmg`, `admin`, `logs`, `admin_simcc`) garantindo compatibilidade retroativa total com o frontend por meio de fallbacks graciosos (`[]`, `null`, `0`). Adiciona a tabela única `researcher_institution_data` vinculada por `researcher_id` para armazenar dados institucionais padronizados (`zip_code`, `work_regime`) e flexíveis (`custom_attributes` JSONB), com script de ingestão reutilizável para CSVs de instituições (iniciando com `ufrb.csv`), e aplica com segurança a constraint `lattes_id NOT NULL` na tabela `researcher` com limpeza prévia de dados inválidos.

## Technical Context

**Language/Version**: Python 3.13
**Primary Dependencies**: FastAPI, SQLAlchemy 2.0 (async), Alembic, Pydantic v2, Polars / stdlib csv, structlog
**Storage**: PostgreSQL 17 com pgvector + unaccent
**Testing**: pytest, testcontainers, factory_boy
**Target Platform**: Linux / Docker (compose)
**Project Type**: REST API Backend
**Performance Goals**: Tempo de resposta do enrichment em lote < 100ms para 50 pesquisadores
**Constraints**: 100% de compatibilidade com os contratos JSON esperados pelo frontend existente; conformidade com Ruff (79 cols, aspas simples)
**Scale/Scope**: ~900 pesquisadores por instituição nos CSVs; tabela `researcher` com ~10k+ registros

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Princípio | Avaliação de Conformidade | Status |
|---|---|---|
| **I. Arquitetura em Camadas** | Routers apenas expõem endpoints; Services orquestram `enrich_researchers`; Repositories executam as consultas e fallbacks | ✅ PASS |
| **II. Query Object Pattern** | Consultas a dados institucionais parametrizadas e tipadas | ✅ PASS |
| **III. Banco Async e Versionado** | Migração Alembic segura (delete prévio + constraint) e modelo `researcher_institution_data` registrado | ✅ PASS |
| **IV. Camada de IA** | Não afeta funcionalidades de IA | ✅ PASS |
| **V. Observabilidade** | Logs estruturados via `structlog` no enrichment e no script de ingestão | ✅ PASS |
| **VI. Qualidade Ruff** | Linhas com até 79 caracteres, aspas simples, tipagem completa | ✅ PASS |
| **VII. Testes em Dois Tiers** | Tier A para fallbacks sem banco + Tier B com testcontainers para migração e upsert | ✅ PASS |
| **VIII. Git Worktree** | Classificada como spec grande (envolve models, repo, service, migration, script, router) → utilizar worktree isolado | ✅ PASS |
| **IX. Commits Atômicos** | Mensagens de commit em português (`feat:`, `migration:`, `test:`, `refactor:`) | ✅ PASS |

## Project Structure

### Documentation (this feature)

```text
specs/002-db-cleanup-legacy-schemas/
├── plan.md              # Este plano de implementação
├── research.md          # Decisões técnicas e trade-offs
├── data-model.md        # Modelagem de dados da nova tabela e alterações
├── quickstart.md        # Roteiro de validação e testes
├── contracts/           # Contratos de API
│   └── researcher-api.md
├── checklists/
│   └── requirements.md  # Checklist de qualidade da especificação
└── tasks.md             # Tarefas de implementação (gerado por /speckit-tasks)
```

### Source Code Impact

```text
src/simcc/
├── core/
│   └── db/
│       └── models/
│           ├── __init__.py               # Remove legados, exporta ResearcherInstitutionData
│           ├── admin.py                  # Desregistrado do table_registry
│           ├── ufmg.py                   # Desregistrado do table_registry
│           ├── researcher.py             # lattes_id: Mapped[str] (não-nulo)
│           └── researcher_institution.py # Novo modelo ResearcherInstitutionData
├── repositories/
│   └── researcher_repo.py                # Fallbacks para list_ufmg_data, list_user_data, get_departament_rt; nova busca de dados institucionais
├── services/
│   └── researcher_service.py             # Injeção de custom_attributes no enrich_researchers
└── schemas/
    └── researcher.py                     # Schema Pydantic atualizado com custom_attributes

scripts/
└── ingest/
    └── ingest_institution_researchers.py # Script CLI reutilizável de ingestão de CSV

migrations/
└── versions/
    └── xxxx_cleanup_legacy_and_add_institution_data.py # Migração Alembic

tests/
├── unit/
│   └── test_researcher_enrichment.py     # Teste Tier A do fallback e mapeamento de custom_attributes
└── integration/
    ├── test_institution_ingest.py        # Teste Tier B de ingestão e upsert do CSV
    └── test_researcher_migration.py      # Teste Tier B da constraint lattes_id NOT NULL
```

## Implementation Phases

### Fase 1: Desacoplamento de Schemas Legados e Modelos
1. Remover o registro de `admin.py` e `ufmg.py` de `src/simcc/core/db/models/__init__.py`.
2. Implementar fallbacks graciosos em `src/simcc/repositories/researcher_repo.py` para as funções `list_ufmg_data_by_ids`, `list_user_data_by_lattes_ids` e `get_departament_rt`.
3. Ajustar `src/simcc/services/researcher_service.py` para tratar ausência de dados legados sem erros.

### Fase 2: Modelagem e Migração Alembic
1. Atualizar modelo `Researcher` (`lattes_id: Mapped[str]`).
2. Criar modelo `ResearcherInstitutionData` em `src/simcc/core/db/models/researcher_institution.py` e registrar no `__init__.py`.
3. Criar migração Alembic com:
   - `DELETE FROM researcher WHERE lattes_id IS NULL;`
   - `ALTER TABLE researcher ALTER COLUMN lattes_id SET NOT NULL;`
   - `CREATE TABLE researcher_institution_data (...);`
   - `CREATE UNIQUE INDEX ix_researcher_institution_data_researcher_id ...;`
4. Implementar `downgrade()` seguro.

### Fase 3: Enriquecimento de Dados Institucionais
1. Adicionar função `list_institution_data_by_researcher_ids` em `researcher_repo.py`.
2. Integrar a consulta de dados institucionais na função `enrich_researchers` em `researcher_service.py`.
3. Atualizar schemas Pydantic de resposta para incluir o campo `custom_attributes: Optional[Dict[str, Any]] = None`.

### Fase 4: Script de Ingestão de CSV Reutilizável
1. Criar `scripts/ingest/ingest_institution_researchers.py` com suporte a CLI (`--file`).
2. Implementar leitura do CSV, resolução de `researcher_id` via `lattes_id`, separação de colunas padronizadas (`zip_code`, `work_regime`), descarte de `name` e empacotamento de colunas extras em `custom_attributes` JSONB.
3. Implementar upsert idempotente no PostgreSQL.
4. Exibir relatório detalhado de execução.

### Fase 5: Testes Automatizados e Validação
1. Escrever testes de unidade (Tier A) para o enriquecimento e tratamento de fallbacks.
2. Escrever testes de integração (Tier B) para o script de ingestão e para a migração.
3. Validar com `storage/researchers/ufrb.csv`.
4. Executar `ruff check .` e `ruff format .`.
