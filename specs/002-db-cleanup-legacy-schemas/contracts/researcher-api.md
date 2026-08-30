# API Contracts: Researcher Enrichment & Institutional Data

**Feature**: `002-db-cleanup-legacy-schemas`
**Date**: 2026-08-29

## 1. Schema do Objeto Researcher Enriquecido (`GET /researchers`, `GET /researcher/{id}`)

### Payload de Resposta (Trecho do Objeto de Pesquisador)

```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "name": "ADRIANO ANUNCIACAO OLIVEIRA",
  "lattes_id": "8343393957854863",
  "graduate_programs": [],
  "research_groups": [],
  "subsidy": [],
  "departments": [],
  "ufmg": null,
  "user": null,
  "custom_attributes": {
    "zip_code": "44300-000",
    "work_regime": "DE",
    "siape": "1673892",
    "department": "CAHL",
    "city": "CACHOEIRA"
  }
}
```

### Regras de Contrato:
1. **`departments`**: Sempre `list` (`[]` quando dados do schema legado não existirem).
2. **`ufmg`**: `null` quando schema `ufmg` não existir.
3. **`user`**: `null` quando schema `admin` não existir.
4. **`custom_attributes`**:
   - `null` se não houver registro em `researcher_institution_data`.
   - Objeto contendo os dados consolidados (`zip_code`, `work_regime` + atributos flexíveis do JSONB) caso exista registro.

---

## 2. Contrato do Endpoint de Métricas RT (`GET /researcher/departament-rt`)

### Payload de Resposta
```json
{
  "teachers": 0,
  "technicians": 0
}
```
> Retorna contagens numéricas (`0` por padrão caso o schema `ufmg` não esteja presente).
