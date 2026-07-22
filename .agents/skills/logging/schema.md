# Schema do Log Estruturado

Este documento define o contrato oficial de schema para todos os logs gerados na plataforma SimCC.

## 1. Campos Globais e Tipos

Todos os logs produzidos na aplicação devem conter exatamente e estritamente a seguinte estrutura de chaves de primeiro nível:

| Campo | Tipo | Descrição | Obrigatório? |
| :--- | :--- | :--- | :--- |
| `timestamp` | `string` | Data e hora no formato ISO-8601 UTC (ex: `YYYY-MM-DDTHH:MM:SS.mmmmmmZ`) | Sim |
| `level` | `string` | Nível de log em caixa baixa: `debug`, `info`, `warning`, `error`, `critical` | Sim |
| `application` | `string` | Nome identificador do microserviço / aplicação (padrão: `simcc`) | Sim |
| `category` | `string` | Categoria do evento (HTTP, Database, Routine, etc.) | Sim |
| `event` | `string` | Nome lógico padronizado do evento (ex: `request.received`) | Sim |
| `message` | `string` | Texto legível contendo a descrição resumida do ocorrido | Sim |
| `request_id` | `string` \| `null` | UUID único da requisição ou rotina ativa no contexto | Sim (pode ser `null`) |
| `duration` | `number` \| `null` | Duração da execução em milissegundos | Sim (pode ser `null`) |
| `data` | `object` | Dicionário contendo metadados do contexto ou dados dinâmicos | Sim (pode ser `{}`) |

---

## 2. Regras para o uso do objeto `data`

O dicionário `data` armazena informações extras relevantes para diagnóstico técnico.

### Metadados Automáticos de Contexto (Injetados sob `data`):
*   `environment`: Ambiente ativo (ex: `production`, `development`, `test`).
*   `hostname`: Identificação do servidor físico ou container.
*   `user_id`: ID do usuário autenticado (se disponível).
*   `route`: Endpoint HTTP da rota ativa (se disponível).
*   `method`: Método HTTP associado (se disponível).
*   `routine_name`: Nome da rotina CLI em execução (se aplicável).

### Dados Dinâmicos Customizados:
Qualquer outro dado passado pelo desenvolvedor (como IDs de entidade, métricas específicas, mensagens de erro detalhadas) **deve** ser alocado dentro de `data` para manter a raiz do JSON limpa e livre de poluição.

---

## 3. Exemplo JSON

```json
{
  "timestamp": "2026-07-22T17:23:33.509614Z",
  "level": "error",
  "application": "simcc",
  "category": "http",
  "event": "request.error",
  "message": "Request error: GET /test-logging-trace-error - ConnectionTimeout",
  "request_id": "f6eeb62f-5e38-467c-a7c5-eebacb4725a7",
  "duration": 70.58,
  "data": {
    "environment": "development",
    "hostname": "srv-prod-01",
    "user_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "route": "/test-logging-trace-error",
    "method": "GET",
    "routine_name": null,
    "error_message": "ConnectionTimeout"
  }
}
```

---

## 4. Versionamento do Schema
*   **Versão Atual**: `1.0.0`
*   Qualquer quebra de compatibilidade (remoção ou renomeação de campos raiz) exige incremento da versão do schema, alteração de coletores secundários e notificação da equipe de monitoramento.

## 5. Validações Obrigatórias
*   A serialização de tipos customizados (como classes Pydantic, datetimes ou UUIDs) deve ser tratada de forma transparente para evitar falhas silenciosas na escrita dos logs (resolvida em `handlers.py` com o uso de `json.dumps(..., default=str)`).
*   Logs que não contêm dicionário `data` ou campos obrigatórios válidos falharão na validação.
