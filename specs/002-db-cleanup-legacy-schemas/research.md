# Phase 0 Research: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Feature**: `002-db-cleanup-legacy-schemas`
**Date**: 2026-08-29

## 1. Desacoplamento dos Schemas Legados (`ufmg`, `admin`, `logs`, `admin_simcc`)

### Decision
- **Camada de Repositório / Serviço**: Substituir o acesso direto às tabelas `ufmg.*` e `admin.users` por blocos resilientes com captura de exceções de banco de dados (`ProgrammingError`, `UndefinedTableError`, `OperationalError`) ou stubs de fallback direto que retornam dados neutros (`[]`, `None`, `0`).
- **Funções Específicas**:
  - `researcher_repo.list_ufmg_data_by_ids`: Retornar `[]` com fallback resiliente ou stub.
  - `researcher_repo.list_user_data_by_lattes_ids`: Retornar `[]` com fallback resiliente ou stub.
  - `researcher_repo.get_departament_rt`: Retornar `{"teachers": 0, "technicians": 0}` quando a tabela `ufmg.researcher` / `ufmg.technician` não estiver disponível.
  - `researcher_service.enrich_researchers`: Garantir que campos `departments`, `ufmg`, `user` recebam `[]` ou `None` sem quebrar o fluxo.

### Rationale
O frontend consome esses campos em diversas telas. Manter os campos nas respostas JSON com valores neutros (`[]`, `null`, `0`) garante 100% de compatibilidade retroativa com a UI sem exigir que os schemas legados existam no banco do SIMCC.

### Alternatives Considered
- *Remover campos do JSON*: Quebraria o frontend que espera a estrutura tipada.
- *Manter schemas mockados como tabelas vazias no banco*: Poluição de schema desnecessária; a responsabilidade de fallback deve ser do backend.

---

## 2. Remoção dos Modelos Legados do `table_registry`

### Decision
- Desregistrar `Admin*` e `Ufmg*` de `src/simcc/core/db/models/__init__.py`.
- Remover decorators `@table_registry.mapped_as_dataclass` dos arquivos `admin.py` e `ufmg.py` (ou isolá-los fora do registro do Alembic).

### Rationale
`migrations/env.py` usa `target_metadata = table_registry.metadata` e `include_schemas = True`. Ao remover os modelos legados do registro, o Alembic passa a gerenciar unicamente as tabelas do domínio central do SIMCC no schema `public`.

---

## 3. Modelo e Tabela `researcher_institution_data`

### Decision
- Criar o modelo SQLAlchemy em `src/simcc/core/db/models/researcher_institution.py` com decorator `@table_registry.mapped_as_dataclass`.
- Estrutura:
  - `id`: UUID (PK, server_default=`gen_random_uuid()`)
  - `researcher_id`: UUID (FK `researcher.id`, `ondelete='CASCADE'`, `unique=True`, indexed)
  - `zip_code`: Optional[str] (VARCHAR, nullable=True)
  - `work_regime`: Optional[str] (VARCHAR, nullable=True)
  - `custom_attributes`: Optional[dict] (JSONB, nullable=True, default=dict)
- Exportar em `src/simcc/core/db/models/__init__.py`.

### Rationale
A combinação de colunas dedicadas para atributos padronizados (`zip_code`, `work_regime`) com uma coluna `JSONB` (`custom_attributes`) oferece flexibilidade total para atributos heterogêneos de diferentes instituições (UFRB, UFBA, UESC, etc.) sem requerer novas migrações DDL.

---

## 4. Estratégia de Migração: `lattes_id NOT NULL`

### Decision
Criar uma migration manual/autogerada pelo Alembic estruturada em 2 passos sequenciais no `upgrade()`:
1. `DELETE FROM researcher WHERE lattes_id IS NULL;` (registros órfãos são removidos em cascata graças aos `ondelete='CASCADE'`).
2. `ALTER TABLE researcher ALTER COLUMN lattes_id SET NOT NULL;`

No `downgrade()`:
1. `ALTER TABLE researcher ALTER COLUMN lattes_id DROP NOT NULL;`

### Rationale
Aplicar `SET NOT NULL` diretamente falharia caso existissem registros com `lattes_id IS NULL`. A limpeza prévia no mesmo bloco de transação garante atomicidade da migração.

---

## 5. Script de Ingestão CSV Reutilizável

### Decision
- Implementar `scripts/ingest/ingest_institution_researchers.py` usando `asyncio` e SQLAlchemy async engine.
- Aceitar argumento CLI `--file` ou `-f` (ex.: `storage/researchers/ufrb.csv`).
- Parsing via biblioteca padrão `csv` / `polars`.
- Fluxo:
  1. Carregar CSV e verificar presença da coluna `lattes_id`.
  2. Consultar `researcher.id` a partir dos `lattes_id` não vazios em lote.
  3. Mapear `zip_code` e `work_regime` para colunas dedicadas.
  4. Extrair atributos restantes (ex.: `siape`, `department`, `city`) para o dict `custom_attributes`, descartando expressamente `name`.
  5. Executar `INSERT ... ON CONFLICT (researcher_id) DO UPDATE` (upsert).
  6. Imprimir relatório com métricas: total de linhas, processadas com sucesso, ignoradas por falta de match ou identificador vazio.

### Rationale
Garante idempotência (execuções subsequentes atualizam registros existentes sem duplicação) e alta performance via batch operations.
