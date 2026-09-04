# Contract: OpenTelemetry Spans & Trace Hierarchy

**Feature**: `002-opentelemetry-tracing`  
**Date**: 2026-09-03  
**Status**: Formal Interface Contract

Este contrato define a hierarquia estrita de spans, convenções de nomenclatura e dicionário de atributos para rastreamento distribuído no SIMCC Backend.

---

## 1. Estrutura da Árvore de Spans (Trace Tree)

### A. Fluxo de Chat em Lote (`POST /ai/chat/ask`) com Cache Miss

```text
Trace: {trace_id}
POST /ai/chat/ask                                  (SpanKind.SERVER)
│
├── redis.get                                      (SpanKind.CLIENT)
│
└── ai.pipeline                                    (SpanKind.INTERNAL)
    │
    ├── ai.planner                                 (SpanKind.INTERNAL)
    │
    ├── pgvector.search                            (SpanKind.CLIENT)
    │   └── db.query (PostgreSQL)                  (SpanKind.CLIENT)
    │
    ├── ai.cutoff                                  (SpanKind.INTERNAL)
    │
    ├── ai.synthesis                               (SpanKind.INTERNAL)
    │   └── HTTP POST api.openai.com/v1/chat       (SpanKind.CLIENT)
    │
    └── redis.set                                  (SpanKind.CLIENT)
```

---

### B. Fluxo de Chat com Cache Hit

```text
Trace: {trace_id}
POST /ai/chat/ask                                  (SpanKind.SERVER)
│
└── redis.get                                      (SpanKind.CLIENT) [cache.hit=true]
```

---

### C. Fluxo de Chat com Erro na Síntese (Ex: Timeout no LLM)

```text
Trace: {trace_id}
POST /ai/chat/ask                                  (SpanKind.SERVER, StatusCode.ERROR)
│
└── ai.pipeline                                    (SpanKind.INTERNAL, StatusCode.ERROR)
    │
    ├── ai.planner                                 (SpanKind.INTERNAL, StatusCode.OK)
    ├── pgvector.search                            (SpanKind.CLIENT, StatusCode.OK)
    │
    └── ai.synthesis                               (SpanKind.INTERNAL, StatusCode.ERROR)
        ├── HTTP POST api.openai.com/v1/chat       (SpanKind.CLIENT, StatusCode.ERROR)
        └── Event: exception (type: TimeoutError)
```

---

## 2. Atributos Semânticos por Span

### A. Span HTTP Raiz (`POST /ai/chat/ask`)
* `http.request.method`: `"POST"`
* `http.route`: `"/ai/chat/ask"`
* `http.response.status_code`: `200`
* `url.path`: `"/ai/chat/ask"`
* `service.name`: `"simcc-back"`
* `service.version`: `"4.5.0"`

### B. Span `ai.pipeline`
* `ai.pipeline.name`: `"maria_chat"`
* `ai.model`: `"gpt-4o-mini"`
* `ai.cache_hit`: `false`
* `ai.query_length`: `48` *(número de caracteres, NÃO o texto integral)*
* `ai.intent`: `"researcher_search"`
* `ai.total_duration_ms`: `412.5`

### C. Span `ai.planner`
* `ai.stage`: `"planner"`
* `ai.extracted_intent`: `"researcher_search"`
* `ai.filter_count`: `1`
* `ai.has_semantic_terms`: `true`

### D. Span `ai.retrieval` / `pgvector.search`
* `ai.stage`: `"retrieval"`
* `db.system`: `"postgresql"`
* `db.name`: `"simcc_db"`
* `db.operation`: `"similarity_search"`
* `ai.retrieval.documents_found`: `12`

### E. Span `ai.cutoff`
* `ai.stage`: `"cutoff"`
* `ai.retrieval.cutoff_threshold`: `0.65`
* `ai.retrieval.documents_after_cutoff`: `4`
* `ai.retrieval.dropped_count`: `8`

### F. Span `ai.synthesis`
* `ai.stage`: `"synthesis"`
* `ai.prompt_variation`: `"variation_b_reduced"`
* `gen_ai.usage.input_tokens`: `640`
* `gen_ai.usage.output_tokens`: `185`

---

## 3. Cláusula de Conformidade de Privacidade e Segurança

1. **PROIBIDO**:
   * O valor do atributo `query` jamais conterá o texto digitado pelo usuário nos spans de produção;
   * O atributo `prompt` jamais conterá o template completo ou os dados dos pesquisadores;
   * O atributo `response` jamais conterá a resposta textual final da MarIA;
   * O atributo `db.statement` jamais conterá queries SQL com parâmetros de usuários.
2. **PERMITIDO**:
   * Identificadores de estágio (`planner`, `retrieval`, `cutoff`, `synthesis`);
   * Contagens numéricas (`documents_found`, `dropped_count`, contagem de tokens);
   * Flags booleanas (`cache_hit`);
   * Tipos de erro e classes de exceção (`TimeoutError`, `AuthenticationError`).
