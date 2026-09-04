# Research & Technical Decisions: OpenTelemetry no SIMCC

**Feature**: `002-opentelemetry-tracing`  
**Date**: 2026-09-03  
**Status**: Completed

Este documento consolida as decisões arquiteturais, padrões e investigações tecnológicas para a implementação da observabilidade vendor-neutral com OpenTelemetry no SIMCC Backend.

---

## 1. Pacotes e Dependências do OpenTelemetry para Python

### Decisão
Instalar os pacotes fundamentais em conjunto controlado e modular via Poetry:
1. `opentelemetry-api` (>=1.30.0)
2. `opentelemetry-sdk` (>=1.30.0)
3. `opentelemetry-exporter-otlp` (>=1.30.0)
4. `opentelemetry-instrumentation-fastapi` (>=0.51b0)
5. `opentelemetry-instrumentation-httpx` (>=0.51b0)
6. `opentelemetry-instrumentation-redis` (>=0.51b0)
7. `opentelemetry-instrumentation-sqlalchemy` (>=0.51b0)

### Justificativa
A documentação oficial do OpenTelemetry Python separa o núcleo da API/SDK dos plugins de instrumentação de frameworks. Instalar os pacotes específicos para FastAPI, SQLAlchemy, Redis e HTTPX evita carregar bibliotecas desnecessárias e garante compatibilidade com o stack assíncrono do projeto.

### Alternativas Rejeitadas
* **`opentelemetry-distro` / `opentelemetry-bootstrap`**: Executa auto-instrumentação cega via agente CLI externo. Rejeitado porque retira o controle granular de inicialização da aplicação, dificulta a injeção de atributos customizados da MarIA e pode introduzir overhead em bibliotecas não mapeadas.
* **Langfuse / Arize Phoenix direto**: Rejeitado pelo usuário nesta etapa para evitar vendor lock-in e manter a arquitetura puramente vendor-neutral via OpenTelemetry padronizado.

---

## 2. Estrutura Modular e Localização do Código (`simcc/core/telemetry/`)

### Decisão
Centralizar toda a infraestrutura de telemetria no módulo [`src/simcc/core/telemetry/`](file:///home/jaspion/Observatorio/Simcc/simcc-back/src/simcc/core/telemetry/):
* `config.py`: Definição de `TelemetrySettings` (habilitado, endpoint OTLP, tipo de exporter, taxa de amostragem).
* `tracing.py`: Inicialização do `TracerProvider`, `Resource`, `SpanProcessor` e instrumentadores automáticos.
* `metrics.py`: Inicialização do `MeterProvider`, definição de medidores (histogramas e contadores de IA e HTTP).
* `__init__.py`: Ponto de entrada com função unificada `init_telemetry(app: FastAPI)`.

### Justificativa
O arquivo `main.py` deve permanecer enxuto, chamando apenas `init_telemetry(app)`. Quando o destino da telemetria for alternado (Console → OTLP → Collector → Tempo/Prometheus), nenhuma linha do código de negócio ou rotas precisará ser alterada.

### Alternativas Rejeitadas
* **Configuração embutida em `main.py`**: Gera acoplamento e dificulta o reaproveitamento em rotinas de background e testes unitários.

---

## 3. Identidade do Serviço e Semantic Conventions

### Decisão
Adotar estritamente as convenções semânticas oficiais do OpenTelemetry para recursos:
* `service.name`: `"simcc-back"`
* `service.namespace`: `"simcc"`
* `service.version`: `"4.5.0"` (lida dinamicamente do `pyproject.toml`)
* `deployment.environment.name`: `"development"` / `"production"` / `"test"` (via `ENVIRONMENT`)

### Justificativa
Garante consistência imediata ao enviar dados para qualquer coletor ou plataforma de visualização padrão (Jaeger, Grafana Tempo, SigNoz, Datadog), eliminando nomes proprietários ou inconsistentes.

---

## 4. Evolução do `AITracer` para Spans Semânticos da MarIA

### Decisão
Manter e evoluir a classe [`AITracer`](file:///home/jaspion/Observatorio/Simcc/simcc-back/src/simcc/ai/telemetry/tracer.py), transformando seu context manager `trace_stage(stage_name)` em um criador de spans ativos do OpenTelemetry:
* Span pai: `ai.pipeline` (iniciado em `ask_chat` ou `stream_chat`).
* Spans filhos:
  * `ai.planner`: Análise de intenção e extração de filtros semânticos.
  * `ai.retrieval`: Busca híbrida vetorial no `pgvector`.
  * `ai.cutoff`: Filtragem por distância cosseno e triagem de ruído.
  * `ai.synthesis`: Invocação e streaming de resposta do modelo LLM.

### Justificativa
Preserva 100% da telemetria e dos logs estruturados existentes no `tracer.py`, enquanto produz uma árvore de spans visível e rastreável no OpenTelemetry.

---

## 5. Política de Privacidade e Sanitização de Dados em Traces

### Decisão
Estabelecer como diretriz mandatória:
1. **NÃO registrar**:
   * Conteúdo integral de prompts e templates;
   * Resposta completa da MarIA;
   * Conteúdo textual integral de artigos e patentes;
   * Consultas SQL parametrizadas com valores textuais de usuários em produção;
   * Tokens, chaves de API ou dados de identificação pessoal (CPF, e-mail).
2. **SIM registrar**:
   * `ai.model`: ex. `"gpt-4o-mini"`;
   * `ai.intent`: ex. `"researcher_search"`;
   * `ai.cache_hit`: booleano (`true`/`false`);
   * `ai.retrieval.documents_found`: contagem numérica;
   * `ai.retrieval.documents_after_cutoff`: contagem numérica;
   * `ai.retrieval.cutoff_threshold`: ex. `0.65`;
   * `gen_ai.usage.input_tokens` e `gen_ai.usage.output_tokens`.

### Justificativa
Garante total conformidade com a LGPD e o princípio de minimização de dados, reduz drasticamente o volume e custo de tráfego de telemetria e evita o vazamento acidental de dados sensíveis.

---

## 6. Governança de Métricas e Prevenção de Alta Cardinalidade

### Decisão
Definir um conjunto estrito e controlado de métricas:
1. `http.server.request.duration` (Histograma, dimensões: `http.request.method`, `http.route`, `http.response.status_code`, `environment`).
2. `simcc.ai.requests` (Contador, dimensões: `model`, `intent`, `cache_hit`, `status`).
3. `simcc.ai.duration` (Histograma de latência por estágio, dimensões: `stage`, `status`).
4. `simcc.ai.tokens` (Contador, dimensões: `model`, `type` [`input` | `output`]).

**Regra Estrita**: `user_id`, `request_id`, `trace_id` e texto de busca **jamais** devem figurar como rótulos de métricas.

---

## 7. Coexistência Não Destrutiva com Logs JSONL

### Decisão
O sistema atual de logs JSONL (`src/simcc/core/logging/`) permanecerá como a espinha dorsal de logs locais da aplicação.
* No `format_schema_processor` do structlog, injetar o `trace_id` e `span_id` ativos a partir do contexto do OpenTelemetry (`opentelemetry.trace.get_current_span().get_span_context()`).
* Isso permite correlacionar qualquer linha de log do arquivo diário `.jsonl` com o trace visual correspondente no OpenTelemetry.
