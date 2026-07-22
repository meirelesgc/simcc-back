# Padronização de Logs em Rotas HTTP (APIs)

Este documento orienta sobre a padronização e o comportamento de observabilidade no ciclo de vida de requisições HTTP da API.

## 1. O Middleware de Logging

Toda a captura de dados de tráfego HTTP é de responsabilidade exclusiva do **[LoggingMiddleware](../../../src/simcc/core/logging/middleware.py)**. 
Os endpoints individuais (routers) **não devem** emitir logs HTTP manualmente.

### Comportamento do Middleware:
1.  **Intercepta a entrada**: Captura a requisição, analisa se existe o cabeçalho `X-Request-ID`. Se não existir, gera um UUID4.
2.  **Configura o ContextVars**: Injeta `request_id`, `route` (caminho), e `method` (GET, POST, etc.) no contexto assíncrono.
3.  **Registra a entrada**: Dispara o evento `request.received` chamando a função helper `request_received()`.
4.  **Mede a duração**: Utiliza `time.perf_counter()` para iniciar a contagem temporal com precisão.
5.  **Despacha e monitora exceções**:
    *   **Sucesso**: Dispara o evento `request.finished` chamando `request_finished()` contendo o tempo decorrido calculado. Retorna a resposta injetando o cabeçalho `X-Request-ID` para que o cliente consiga rastrear o log posteriormente.
    *   **Falha (Exceção)**: Lança um bloco try-except geral. Captura qualquer erro não tratado, calcula a duração acumulada até ali e emite `request.error` chamando `request_error()`. O erro é logado e a exceção é relançada para o framework resolver.

---

## 2. Eventos Emitidos

Os seguintes eventos de categoria `http` são emitidos de maneira estrita:
*   `request.received`: Informa que uma chamada HTTP foi iniciada.
*   `request.finished`: Informa que a chamada foi processada e respondida com sucesso.
*   `request.error`: Informa que o processamento gerou erro interno e resultará em falha (HTTP 500).

---

## 3. Campos Capturados nos Logs HTTP

### Globais:
*   `request_id`: O UUID associado à request (transmitido no JSON final).
*   `duration`: Milissegundos gastos no processamento completo (apenas para `request.finished` e `request.error`).

### Dentro de `data`:
*   `route`: O caminho acionado (ex: `/api/v1/researchers/find`).
*   `method`: O verbo HTTP utilizado (ex: `GET`).
*   `user_id`: ID do usuário autenticado no momento do processamento (opcional).
*   `error_message`: Mensagem descritiva da exceção capturada (apenas para `request.error`).

---

## 4. Rastreabilidade por Request ID

Toda resposta HTTP gerada pela API devolve o cabeçalho de resposta:
`X-Request-ID: <UUID>`

Esse cabeçalho permite que clientes frontend ou integradores externos localizem toda a linha temporal das transações internas que ocorreram durante o processamento daquela requisição única no arquivo JSONL de logs, incluindo erros de banco de dados.
