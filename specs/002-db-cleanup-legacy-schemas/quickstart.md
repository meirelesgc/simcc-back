# Quickstart Guide: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Feature**: `002-db-cleanup-legacy-schemas`
**Date**: 2026-08-29

Este guia descreve os cenários de teste e validação de ponta a ponta da feature.

---

## 1. Pré-requisitos

1. Ambiente com Poetry e Docker disponíveis.
2. Banco de dados PostgreSQL rodando (ou contêiner de testes).

---

## 2. Passo a Passo de Validação

### Passo 1: Executar Migrações do Banco
Aplicar as migrações que realizam o delete prévio de pesquisadores sem `lattes_id`, adicionam a constraint `NOT NULL` e criam a tabela `researcher_institution_data`:

```bash
poetry run alembic upgrade head
```

**Verificação Esperada**:
- `researcher.lattes_id` está com constraint `NOT NULL`.
- Tabela `researcher_institution_data` foi criada no schema `public`.

---

### Passo 2: Executar Script de Ingestão de CSV da UFRB

Executar o script passando o CSV de exemplo da UFRB:

```bash
poetry run python scripts/ingest/ingest_institution_researchers.py --file storage/researchers/ufrb.csv
```

**Verificação Esperada**:
- Saída do script no terminal mostrando:
  - Total de linhas no CSV (~912)
  - Inserções/atualizações realizadas com sucesso
  - Linhas ignoradas (sem match no banco)
- Consulta ao banco confirma inserção na tabela `researcher_institution_data`:
  ```sql
  SELECT * FROM researcher_institution_data LIMIT 5;
  ```

---

### Passo 3: Validar Idempotência da Ingestão

Reexecutar o script com o mesmo CSV:

```bash
poetry run python scripts/ingest/ingest_institution_researchers.py --file storage/researchers/ufrb.csv
```

**Verificação Esperada**:
- Nenhuma duplicação de dados ocorre (upsert atualiza registros existentes).

---

### Passo 4: Executar Testes Automatizados da Aplicação

Executar os testes de unidade e integração:

```bash
poetry run pytest -m "not ai_live"
```

**Verificação Esperada**:
- Todos os testes passam sem erro.
- Testes cobrem o fallback de schemas legados e o enriquecimento de `custom_attributes`.

---

### Passo 5: Testar Endpoint de Pesquisadores

Iniciar a aplicação e fazer uma requisição de busca de pesquisadores:

```bash
poetry run fastapi dev src/simcc/main.py
```

Requisição:
```bash
curl -X GET "http://localhost:8000/researchers?page=1&lenght=10"
```

**Verificação Esperada**:
- Resposta `200 OK`.
- Pesquisadores com dados institucionais retornam o objeto `custom_attributes` preenchido.
- `departments` retorna `[]`, `ufmg` retorna `null`, `user` retorna `null`.
