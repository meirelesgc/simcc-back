# Schema do Log Estruturado

Este documento define o contrato oficial de schema para todos os logs gerados na plataforma SimCC.

## 1. Campos Globais e Tipos

Todos os logs produzidos na aplicação devem conter exatamente e estritamente a seguinte estrutura de chaves de primeiro nível:

| Campo | Tipo | Descrição | Obrigatório? |
| :--- | :--- | :--- | :--- |
| `timestamp` | `string` | Data e hora no formato ISO-8601 UTC (ex: `YYYY-MM-DDTHH:MM:SS.mmmmmmZ`) | Sim |
| `level` | `string` | Nível de log em caixa baixa: `debug`, `info`, `warning`, `error`, `critical` | Sim |
| `application` | `string` | Nome identificador do microserviço / aplicação (padrão: `simcc`) | Sim |
| `environment` | `string` | Ambiente ativo (ex: `production`, `development`, `test`) | Sim |
| `hostname` | `string` \| `null` | Identificação do servidor físico ou container | Sim (pode ser `null`) |
| `category` | `string` | Categoria do evento (HTTP, Database, Routine, etc.) | Sim |
| `event` | `string` | Nome lógico padronizado do evento (ex: `request.received`) | Sim |
| `message` | `string` | Texto legível contendo a descrição resumida do ocorrido | Sim |
| `request_id` | `string` \| `null` | UUID único da requisição ou rotina ativa no contexto | Sim (pode ser `null`) |
| `duration` | `number` \| `null` | Duração da execução em milissegundos | Sim (pode ser `null`) |
| `data` | `object` | Dicionário contendo metadados estruturados por categoria | Sim |

---

## 2. Regras para o uso do objeto `data` (Padronização por Categoria)

Para facilitar a modelagem em ferramentas de analytics (ex: PowerBI) e evitar chaves dinâmicas imprevisíveis por evento, o bloco `data` deve seguir um esquema fixo e padronizado **por categoria**, contendo todas as chaves predefinidas mesmo que com valor `null`.

### A. Categoria `http`
Bloco `data` fixado com as chaves:
*   `route`: Endpoint HTTP acessado (ex: `/researcherName`).
*   `method`: Método HTTP (ex: `GET`, `POST`, `OPTIONS`).
*   `user_id`: UUID do usuário autenticado no contexto (ou `null`).
*   `error_message`: Mensagem de exceção caso ocorra um erro (ou `null`).

### B. Categoria `database`
Bloco `data` fixado com as chaves:
*   `database_name`: Nome da base de dados afetada.
*   `operation_name`: Função ou repositório que chamou a query.
*   `error_message`: Detalhamento técnico da falha no BD.
*   `sql`: Comando SQL executado (apenas populado em modo `DEBUG`, caso contrário `null`).

### C. Categoria `routine`
Bloco `data` fixado com as chaves:
*   `routine_name`: Nome identificador do script ou tarefa executada.
*   `error_message`: Mensagem de erro caso ocorra uma falha (ou `null`).

### D. Categoria `system` e Outras
Utiliza esquema dinâmico contendo apenas as chaves adicionais enviadas livremente pelo desenvolvedor na chamada do log, além de herdar os campos do contexto ativo (`route`, `method`, `user_id`, `routine_name`) se estes estiverem preenchidos no escopo da requisição.

---

## 3. Exemplo JSON (HTTP Error)

```json
{
  "timestamp": "2026-07-22T17:23:33.509614Z",
  "level": "error",
  "application": "simcc",
  "environment": "development",
  "hostname": "srv-prod-01",
  "category": "http",
  "event": "request.error",
  "message": "Request error: GET /test-logging-trace-error - ConnectionTimeout",
  "request_id": "f6eeb62f-5e38-467c-a7c5-eebacb4725a7",
  "duration": 70.58,
  "data": {
    "route": "/test-logging-trace-error",
    "method": "GET",
    "user_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "error_message": "ConnectionTimeout"
  }
}
```

---

## 4. Versionamento do Schema
*   **Versão Atual**: `1.1.0` (Introdução de campos raiz de infraestrutura e schemas fixados em `data` por categoria).
*   Qualquer quebra de compatibilidade (remoção ou renomeação de campos raiz) exige incremento da versão do schema, alteração de coletores secundários e notificação da equipe de monitoramento.

## 5. Validações Obrigatórias
*   A serialização de tipos customizados (como classes Pydantic, datetimes ou UUIDs) deve ser tratada de forma transparente para evitar falhas silenciosas na escrita dos logs (resolvida em `handlers.py` com o uso de `json.dumps(..., default=str)`).
*   Os coletores analíticos assumem a estrutura estática por categoria descrita acima para junções em bancos de dados relacionais e PowerBI.
