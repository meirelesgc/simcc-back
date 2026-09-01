# Data Model: Melhorias na Interação com IA e Cache Distribuído

## 1. Entidades Conceituais e DTOs

### 1.1 `ChatRequest` (Entrada)
Representa o payload enviado pelo cliente HTTP.
- `query` (str, obrigatório): Texto da pergunta ou consulta em linguagem natural.
- `session_id` (str, opcional): Identificador único da sessão/mensagem para correlação de eventos.

### 1.2 `ChatResponse` (Saída em Lote)
Mantém rigorosa compatibilidade com o contrato atual do frontend.
- `answer` (str): Texto sintetizado gerado pela MarIA em Markdown.
- `intent` (str): Intenção identificada pelo `QueryPlanner` (`production_search`, `researcher_search`, `researcher_profile`, `researcher_comparison`, `aggregation`, `general_question`).
- `filters_extracted` (dict): Dicionário com os filtros relacionais extraídos da pergunta.
- `researchers` (list[dict]): Lista de pesquisadores relevantes que passaram na linha de corte.
- `productions` (list[dict]): Lista de produções científicas/tecnológicas que passaram na linha de corte.
- `sources` (list[str]): Fontes e referências citadas no contexto.

### 1.3 `ChatStreamEvent` (Eventos SSE de Streaming)
Estrutura de cada chunk emitido na rota `/ai/chat/ask/stream`.
- `type` (ChatStreamEventType): Enum com valores `metadata`, `delta`, `error`, `done`.
- `message_id` (str): Identificador da mensagem.
- `data` (dict, opcional): Metadados de UI (`SearchUIMetadata`) emitidos no evento inicial.
- `content` (str, opcional): Fragmento de texto emitido nos eventos `delta`.
- `code` (str, opcional): Código de erro padronizado em caso de falha.
- `message` (str, opcional): Mensagem descritiva de erro.

### 1.4 `SearchUIMetadata` (Metadados de Interface)
- `intent` (str): Intenção do plano.
- `filters` (dict): Filtros aplicados formatados para a interface.
- `researchers` (list[dict]): Lista de pesquisadores retornados.
- `productions` (list[dict]): Lista de produções retornadas.
- `sources` (list[str]): Fontes formatadas.

### 1.5 `QualityCutoffPolicy` (Regra de Triagem Semântica)
- `max_cosine_distance` (float, padrão `0.65`): Limiar máximo aceitável de distância vetorial.
- `min_relevance_score` (float, calculado como `1.0 - distance`): Pontuação mínima de pertinência.

### 1.6 `AICacheRecord` (Estrutura de Cache no Redis)
- `cache_key` (str): Chave no formato `simcc:ai:chat:{canonical_hash}`.
- `response_type` (str): `batch` ou `stream`.
- `created_at` (str, ISO-8601): Timestamp de gravação.
- `ttl_seconds` (int): Tempo de vida do cache (ex: 3600s).
- `payload` (dict): Objeto serializado de `ChatResponse` ou lista de eventos `ChatStreamEvent`.

### 1.7 `AITraceMetrics` (Telemetria e Tracing)
- `request_id` (str): UUID da requisição.
- `query` (str): Texto da consulta original.
- `cache_hit` (bool): Indica se a requisição foi servida a partir do Redis.
- `planner_latency_ms` (float): Tempo gasto no planejamento estruturado.
- `search_latency_ms` (float): Tempo gasto na busca vetorial e relacional.
- `cutoff_dropped_count` (int): Quantidade de itens descartados pela linha de corte.
- `synthesis_latency_ms` (float): Tempo gasto na geração do modelo de linguagem.
- `total_latency_ms` (float): Tempo total de ponta a ponta da pipeline.
