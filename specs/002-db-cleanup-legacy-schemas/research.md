# Phase 0 Research: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Feature**: `002-db-cleanup-legacy-schemas`
**Date**: 2026-08-29 (Atualizado em 2026-08-31)

## 1. Desacoplamento dos Schemas Legados (`ufmg`, `admin`, `admin_ufmg`, `logs`, `admin_simcc`) em Todas as Consultas

### Decision
- **Camada de Repositório / Serviço / Queries**: Substituir o acesso direto às tabelas `ufmg.*` e `admin.*` por consultas desacopladas, stubs e blocos resilientes com captura de exceções de banco de dados (`ProgrammingError`, `UndefinedTableError`, `OperationalError`), garantindo o retorno de dados neutros (`[]`, `None`, `0`) sem lançar erros 500 para o frontend.
- **Mapeamento e Tratamento por Módulo**:
  - **Filtros Comuns & Produção / Métricas / Pós-Graduação (`common.py`, `production_query.py`, `metrics_query.py`, `researcher_query.py`, `graduate_program_query.py`, `external_query.py`)**:
    - Consultas que executam joins com `ufmg.departament_researcher` e `ufmg.departament` ao receberem filtros `dep_id` ou `departament`: os joins devem ser protegidos ou desacoplados de `ufmg.*`. Quando o schema legado não existir, as consultas de contagem/agrupamento por departamento devem lidar graciosamente retornando conjunto vazio ou sem crash de SQL.
  - **Filtros de Pesquisadores (`researcher_query.ResearcherFilterQuery`)**:
    - Remover a subconsulta direta `(SELECT COALESCE(ARRAY_AGG(DISTINCT dep_nom), '{}') FROM ufmg.departament) as departament;` e substituí-la por uma agregação segura ou fallback para `ARRAY[]::TEXT[]` quando a tabela não existir, garantindo que a tela de busca avançada receba `departament: []` no frontend.
  - **Instituições (`institution_query.InstitutionSearchQuery` e `RtMetricsQuery`)**:
    - Substituir as CTEs duras `ufmg_researcher_count` (`FROM ufmg.researcher`) e `technician_count` (`FROM ufmg.technician`) por contagens resilientes com valor padrão `0` (`count_d: 0`, `count_t: 0`).
    - Em `RtMetricsQuery`, retornar `[]` ou contagens seguras sem tentar acessar `ufmg.researcher` / `ufmg.technician`.
  - **Enriquecimento de Pesquisadores (`researcher_repo` & `researcher_service`)**:
    - `researcher_repo.list_ufmg_data_by_ids`: Retornar `[]` com fallback resiliente ou stub.
    - `researcher_repo.list_user_data_by_lattes_ids`: Retornar `[]` com fallback resiliente ou stub.
    - `researcher_repo.get_departament_rt`: Retornar `{"teachers": 0, "technicians": 0}` quando a tabela `ufmg.researcher` / `ufmg.technician` não estiver disponível.
    - `researcher_service.enrich_researchers`: Garantir que campos `departments`, `ufmg`, `user` recebam `[]` ou `None` sem quebrar o fluxo.
  - **Módulo External (`external_query.py`, `external_repo.py`, `external_service.py`)**:
    - `ExternalResearcherSearchQuery`, `DepartmentSearchQuery`, `ResearcherDataQuery`, `TechnicianQuery`: Tratar ausência das tabelas retornando listas vazias `[]` de forma resiliente.
  - **Módulo PowerBI (`powerBi_query.py`, `powerBi_repo.py`, `powerBi_service.py`)**:
    - Endpoints de exportação CSV (`dim_departament.csv`, `dim_departament_researcher.csv`, etc.) devem retornar DataFrames vazios com as colunas esperadas caso as tabelas legadas (`ufmg.*`, `admin.*`) não existam.
  - **Rotinas de Backend (`scripts/routines/abstract_ai.py`)**:
    - Ajustar queries SQL para não depender de tabelas `ufmg.*`.

### Rationale
O frontend consome esses campos em diversas telas e depende da estabilidade dos contratos JSON. Manter os campos nas respostas JSON com valores neutros (`[]`, `null`, `0`) garante 100% de compatibilidade retroativa com a UI sem exigir que os schemas legados existam no banco do SIMCC.

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
