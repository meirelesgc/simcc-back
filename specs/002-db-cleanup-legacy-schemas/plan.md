# Implementation Plan: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Branch**: `002-db-cleanup-legacy-schemas` | **Date**: 2026-08-29 (Revisado em 2026-08-31) | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-db-cleanup-legacy-schemas/spec.md`

## Summary

Esta feature desacopla integralmente a aplicação dos schemas legados (`ufmg`, `admin`, `admin_ufmg`, `logs`, `admin_simcc`) em todas as camadas de queries SQL, repositórios, serviços e rotas, garantindo compatibilidade retroativa total com o frontend por meio de fallbacks graciosos (`[]`, `null`, `0`) e consultas resilientes. Adiciona a tabela única `researcher_institution_data` vinculada por `researcher_id` para armazenar dados institucionais padronizados (`zip_code`, `work_regime`) e flexíveis (`custom_attributes` JSONB), com script de ingestão reutilizável para CSVs de instituições (iniciando com `ufrb.csv`), e aplica com segurança a constraint `lattes_id NOT NULL` na tabela `researcher` com limpeza prévia de dados inválidos.

## Technical Context

**Language/Version**: Python 3.13  
**Primary Dependencies**: FastAPI, SQLAlchemy 2.0 (async), Alembic, Pydantic v2, Polars / stdlib csv, structlog  
**Storage**: PostgreSQL 17 com pgvector + unaccent  
**Testing**: pytest, testcontainers, factory_boy  
**Target Platform**: Linux / Docker (compose)  
**Project Type**: REST API Backend  
**Performance Goals**: Tempo de resposta do enrichment em lote < 100ms para 50 pesquisadores  
**Constraints**: 100% de compatibilidade com os contratos JSON esperados pelo frontend existente; conformidade com Ruff (79 cols, aspas simples)  
**Scale/Scope**: ~900 pesquisadores por instituição nos CSVs; tabela `researcher` com ~10k+ registros; 9+ arquivos de queries SQL impactados  

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Princípio | Avaliação de Conformidade | Status |
|---|---|---|
| **I. Arquitetura em Camadas** | Routers apenas expõem endpoints; Services orquestram lógica; Repositories e Query Objects executam consultas e fallbacks | ✅ PASS |
| **II. Query Object Pattern** | Todas as consultas a dados desacopladas, tipadas e sem concatenação de SQL insegura | ✅ PASS |
| **III. Banco Async e Versionado** | Migração Alembic segura (delete prévio + constraint) e modelo `researcher_institution_data` registrado no schema `public` | ✅ PASS |
| **IV. Camada de IA** | Rotinas de IA (`abstract_ai.py`) desacopladas de schemas legados | ✅ PASS |
| **V. Observabilidade** | Logs estruturados via `structlog` no enrichment e no script de ingestão | ✅ PASS |
| **VI. Qualidade Ruff** | Linhas com até 79 caracteres, aspas simples, tipagem completa | ✅ PASS |
| **VII. Testes em Dois Tiers** | Tier A para fallbacks e queries sem banco + Tier B com testcontainers para migração e upsert | ✅ PASS |
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
└── tasks.md             # Tarefas de implementação
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
├── queries/
│   ├── common.py                         # Desacopla joins de departamento ufmg
│   ├── external_query.py                 # Resiliência para queries de ur, technician, departament
│   ├── graduate_program_query.py         # Desacopla join de departamento
│   ├── institution_query.py              # Remove CTEs hardcoded ufmg.researcher e ufmg.technician
│   ├── metrics_query.py                  # Desacopla joins e subconsultas de departamento
│   ├── powerBi_query.py                  # Fallback para queries legadas de ufmg e admin
│   ├── production_query.py               # Desacopla joins de departamento
│   └── researcher_query.py               # Fallback para departament em ResearcherFilterQuery e joins
├── repositories/
│   ├── common.py                         # Desacopla filtros de departamento legados
│   ├── external_repo.py                  # Fallbacks graciosos para endpoints externos
│   ├── institution_repo.py               # Retornos seguros de contagens
│   ├── powerBi_repo.py                   # Fallbacks para exportações PowerBI
│   └── researcher_repo.py                # Fallbacks para list_ufmg_data, list_user_data, get_departament_rt; nova busca de dados institucionais
├── services/
│   ├── external_service.py               # Garantia de retorno de listas vazias
│   ├── powerBi_service.py                # Exportação de DataFrames vazios para tabelas legadas
│   └── researcher_service.py             # Injeção de custom_attributes no enrich_researchers
└── schemas/
    ├── common.py                         # Schema Pydantic atualizado com custom_attributes
    └── researcher.py                     # Schema Pydantic atualizado com custom_attributes

scripts/
├── ingest/
│   └── ingest_institution_researchers.py # Script CLI reutilizável de ingestão de CSV
└── routines/
    └── abstract_ai.py                    # Atualização para remover dependência direta de ufmg

migrations/
└── versions/
    └── xxxx_cleanup_legacy_and_add_institution_data.py # Migração Alembic

tests/
├── unit/
│   ├── test_legacy_fallbacks.py          # Testes Tier A de fallbacks em repositórios e queries
│   └── test_custom_attributes_enrichment.py # Teste Tier A do mapeamento de custom_attributes
└── integration/
    ├── test_institution_ingest.py        # Teste Tier B de ingestão e upsert do CSV
    └── test_researcher_migration.py      # Teste Tier B da constraint lattes_id NOT NULL
```

## Implementation Phases

### Fase 1: Desacoplamento Global de Schemas Legados e Modelos
1. Remover o registro de `admin.py` e `ufmg.py` de `src/simcc/core/db/models/__init__.py`.
2. Refatorar `src/simcc/queries/institution_query.py` removendo CTEs `ufmg_researcher_count` e `technician_count` ou fornecendo contagens seguras `0`.
3. Refatorar `ResearcherFilterQuery` em `src/simcc/queries/researcher_query.py` para retornar `[]` em `departament` de forma segura.
4. Desacoplar filtros de departamento (`dep_id`, `departament`) em `src/simcc/repositories/common.py`, `src/simcc/queries/metrics_query.py`, `src/simcc/queries/production_query.py`, `src/simcc/queries/graduate_program_query.py`, `src/simcc/queries/external_query.py` e `src/simcc/queries/researcher_query.py`.
5. Implementar fallbacks resilientes em `src/simcc/repositories/researcher_repo.py` para as funções `list_ufmg_data_by_ids`, `list_user_data_by_lattes_ids` e `get_departament_rt`.
6. Implementar fallbacks resilientes em `src/simcc/repositories/external_repo.py` e `src/simcc/repositories/powerBi_repo.py`.
7. Ajustar `src/simcc/services/researcher_service.py`, `src/simcc/services/external_service.py` e `src/simcc/services/powerBi_service.py`.
8. Atualizar rotina `scripts/routines/abstract_ai.py`.

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
1. Escrever testes de unidade (Tier A) para o enriquecimento e tratamento de fallbacks em todas as queries e repositórios.
2. Escrever testes de integração (Tier B) para o script de ingestão e para a migração.
3. Validar com `storage/researchers/ufrb.csv`.
4. Executar `ruff check .` e `ruff format .`.
