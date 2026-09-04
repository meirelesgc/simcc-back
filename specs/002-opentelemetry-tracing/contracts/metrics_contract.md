# Contract: Operational Metrics & Low Cardinality Dimensions

**Feature**: `002-opentelemetry-tracing`  
**Date**: 2026-09-03  
**Status**: Formal Interface Contract

Este contrato define o conjunto fechado de métricas operacionais expostas pelo OpenTelemetry no SIMCC, especificando os tipos de instrumentos, unidades de medida e as dimensões permitidas (restringindo a cardinalidade para retenção histórica sustentável).

---

## 1. Catálogo de Métricas

| Nome da Métrica | Instrumento | Unidade | Descrição | Dimensões Permitidas |
|:---|:---|:---|:---|:---|
| `http.server.request.duration` | `Histogram` | `ms` | Duração das requisições HTTP tratadas pelo servidor | `http.request.method`, `http.route`, `http.response.status_code`, `environment` |
| `simcc.ai.requests` | `Counter` | `1` | Total de interações conversacionais recebidas pela MarIA | `model`, `intent`, `cache_hit`, `status` |
| `simcc.ai.stage.duration` | `Histogram` | `ms` | Latência individual de cada estágio da pipeline conversacional | `stage`, `status` |
| `simcc.ai.cache_hits` | `Counter` | `1` | Contagem de requisições de IA atendidas a partir do Redis | `endpoint`, `status` |
| `simcc.ai.tokens` | `Counter` | `{tokens}` | Quantidade de tokens de LLM consumidos | `model`, `type` (`"input"` \| `"output"`) |
| `simcc.ai.errors` | `Counter` | `1` | Total de falhas capturadas na pipeline de IA | `stage`, `error_type` |

---

## 2. Dicionário de Valores Permitidos por Dimensão

Para evitar explosão combinatória de séries temporais (*high cardinality explosion*):

### `http.route`
Apenas rotas normalizadas de primeiro nível ou parametrizadas pelo framework (ex: `"/health"`, `"/ai/chat/ask"`, `"/ai/chat/ask/stream"`). **Nunca** URLs dinâmicas com queries ou parâmetros crus na string.

### `stage`
Apenas os 4 estágios oficiais:
* `"planner"`
* `"retrieval"`
* `"cutoff"`
* `"synthesis"`

### `model`
Apenas o identificador curto do modelo configurado:
* `"gpt-4o-mini"`
* `"text-embedding-3-small"`

### `intent`
Apenas intenções categorizadas pelo `QueryPlanner`:
* `"researcher_search"`
* `"production_search"`
* `"general_chat"`
* `"unknown"`

### `status`
* `"success"`
* `"error"`

---

## 3. Cláusula de Proibição Estrita de Cardinalidade

**Fica estritamente proibida** a inclusão de qualquer uma das seguintes entidades como dimensão/label em qualquer métrica:
1. `request_id` (UUID único);
2. `trace_id` ou `span_id`;
3. `user_id` ou identificadores de sessão (`session_id`);
4. `query` (texto livre da pergunta digitada);
5. Chave individual completa do Redis;
6. Identificador numérico ou UUID de pesquisadores/produções.
