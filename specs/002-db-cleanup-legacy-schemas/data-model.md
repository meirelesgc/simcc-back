# Data Model: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Feature**: `002-db-cleanup-legacy-schemas`
**Date**: 2026-08-29

## 1. Entidades Modificadas

### `researcher` (Tabela Existente)

| Coluna | Tipo | Nullable | Restrições / Defaults | Descrição |
|---|---|---|---|---|
| `id` | UUID | Não | PK, `gen_random_uuid()` | Identificador único do pesquisador |
| `name` | VARCHAR | Não | | Nome do pesquisador |
| `lattes_id` | VARCHAR | **Não** (alterado) | `NOT NULL` (novo) | Identificador canônico do Lattes |
| ... | ... | ... | ... | Demais colunas mantidas |

---

## 2. Novas Entidades

### `researcher_institution_data` (Nova Tabela)

| Coluna | Tipo | Nullable | Restrições / Defaults | Descrição |
|---|---|---|---|---|
| `id` | UUID | Não | PK, `gen_random_uuid()` | Identificador único do registro |
| `researcher_id` | UUID | Não | FK `researcher.id`, `ON DELETE CASCADE`, `UNIQUE`, `INDEX` | Vínculo 1-para-1 com pesquisador |
| `zip_code` | VARCHAR | Sim | `NULL` | CEP institucional do pesquisador |
| `work_regime` | VARCHAR | Sim | `NULL` | Regime de trabalho (ex.: DE, 40h, 20h) |
| `custom_attributes` | JSONB | Sim | `NULL` / `{}` | Atributos livres e específicos da instituição |

#### Relacionamentos
- `researcher 1 ─── 0..1 researcher_institution_data` (via `researcher_id`)
- Cascade: Deleção de `researcher` remove automaticamente a linha correspondente em `researcher_institution_data`.

---

## 3. Entidades Desacopladas (Legadas)

As seguintes tabelas/schemas não são mais gerenciadas pelo Alembic e o código não depende da sua presença no banco:
- Schema `ufmg`: `departament`, `departament_researcher`, `departament_technician`, `mandate`, `researcher`, `researcher_data`, `technician`.
- Schema `admin`: tabelas administrativas legadas (`users`, etc.).
- Schema `admin_simcc`, `logs`.

---

## 4. Estrutura do JSONB `custom_attributes` (Exemplo UFRB)

```json
{
  "siape": "1673892",
  "department": "CAHL",
  "city": "CACHOEIRA"
}
```

> **Nota**: O campo `name` é explicitamente descartado e não entra no `custom_attributes`. `zip_code` e `work_regime` residem em suas colunas dedicadas.
