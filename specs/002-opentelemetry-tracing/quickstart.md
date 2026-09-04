# Quickstart: Validação e Testes do OpenTelemetry

**Feature**: `002-opentelemetry-tracing`  
**Date**: 2026-09-03  
**Status**: Completed

Este guia descreve os passos práticos para validar localmente o funcionamento do OpenTelemetry no SIMCC Backend, comprovando a geração de traces HTTP, spans da pipeline de IA e coexistência com os logs JSONL.

---

## 1. Pré-requisitos e Configuração

Certifique-se de que os contêineres de PostgreSQL e Redis estão em execução:

```bash
docker compose up -d postgres redis
```

Configure as variáveis de ambiente de telemetria em seu `.env` (ou exporte no terminal):

```bash
export OTEL_ENABLED=true
export OTEL_EXPORTER_TYPE=console   # Exibe spans formatados no terminal
export ENVIRONMENT=development
```

---

## 2. Cenário 1: Validação do Span HTTP Básico (`GET /health`)

### Execução
Inicie o servidor de desenvolvimento:
```bash
poetry run task run
```

Em outro terminal, execute uma chamada ao endpoint de integridade:
```bash
curl -i http://localhost:8000/health
```

### Resultado Esperado no Terminal da API
O console do servidor deverá imprimir o span formatado pelo `ConsoleSpanExporter`:
```text
{
    "name": "GET /health",
    "context": {
        "trace_id": "0x4bf92f3577b34da6a3ce929d0e0e4736",
        "span_id": "0x00f067aa0ba902b7",
        "trace_state": "[]"
    },
    "kind": "SpanKind.SERVER",
    "status": {
        "status_code": "UNSET"
    },
    "attributes": {
        "http.request.method": "GET",
        "http.route": "/health",
        "http.response.status_code": 200,
        "service.name": "simcc-back",
        "service.namespace": "simcc"
    }
}
```

---

## 3. Cenário 2: Validação da Árvore de Spans da MarIA (`POST /ai/chat/ask`)

### Execução
Submeta uma pergunta científica à MarIA:
```bash
curl -X POST http://localhost:8000/ai/chat/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "Pesquisadores na área de energias renováveis na Bahia"}'
```

### Resultado Esperado
No terminal serão emitidos os spans filhos correlacionados ao mesmo `trace_id`:
1. `Span: ai.planner` (Kind: `INTERNAL`)
2. `Span: pgvector.search` (Kind: `CLIENT`, `db.system="postgresql"`)
3. `Span: ai.cutoff` (Kind: `INTERNAL`, `ai.retrieval.cutoff_threshold=0.65`)
4. `Span: ai.synthesis` (Kind: `INTERNAL`, `ai.prompt_variation`)
5. `Span: ai.pipeline` (Kind: `INTERNAL`, englobando toda a execução)
6. `Span: POST /ai/chat/ask` (Kind: `SERVER`, raiz)

---

## 4. Cenário 3: Validação do Replay de Cache (Cache Hit)

### Execução
Repita imediatamente a mesma requisição anterior:
```bash
curl -X POST http://localhost:8000/ai/chat/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "Pesquisadores na área de energias renováveis na Bahia"}'
```

### Resultado Esperado
* O tempo de resposta total deve ser inferior a 50ms;
* O trace exibirá apenas o span de recuperação de cache no Redis (`cache.hit=true`), **sem disparar** os spans de busca vetorial nem síntese no modelo de linguagem.

---

## 5. Cenário 4: Coexistência com Logs JSONL

### Execução
Verifique o arquivo diário de log gerado no diretório `logs/`:
```bash
tail -n 5 logs/$(date +%Y-%m-%d).jsonl
```

### Resultado Esperado
* O log continua sendo gravado no formato rigoroso JSONL da Constituição do SIMCC;
* As linhas de log referentes à requisição de teste contêm o `request_id` e o `trace_id` preenchidos e coincidentes com o trace gerado pelo OpenTelemetry.

---

## 6. Cenário 5: Execução da Suíte de Testes Automatizados

Valide os testes unitários e de integração de telemetria sem necessidade de Collector externo:
```bash
poetry run pytest tests/unit/telemetry/ tests/integration/telemetry/ -v
```
