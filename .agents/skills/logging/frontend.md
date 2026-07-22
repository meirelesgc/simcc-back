# Integração de Logs com o Frontend (Estratégia Futura)

Este documento descreve as diretrizes para correlacionar logs gerados na interface do usuário (Frontend) com os logs do servidor backend.

## 1. Correlação por Request ID

A chave para manter a rastreabilidade fim a fim (da ação do usuário na tela até a consulta no banco de dados) é o uso consistente do cabeçalho `X-Request-ID`.

```
Usuário clica em salvar ──> Frontend gera Request ID ──> Envia via Headers HTTP ──> Backend herda no ContextVars
```

### Regras para o Frontend:
1.  **Geração na Origem**: O Frontend (Next.js, Vue, etc.) deve gerar um identificador único (UUID v4) para cada requisição de rede ou ação do usuário.
2.  **Injeção nos Headers**: Injetar esse identificador no cabeçalho HTTP de todas as chamadas feitas para as APIs do backend:
    `X-Request-ID: <UUID>`
3.  **Herança no Servidor**: O middleware HTTP do backend interceptará esse header e preencherá automaticamente as variáveis de contexto de log.

---

## 2. Eventos Previstos (Categoria `frontend`)

Para capturar falhas que ocorrem exclusivamente na tela, prevemos a categoria de logs `frontend` contendo eventos como:
*   `frontend.click`: Registro de interações-chave na tela.
*   `frontend.error`: Exceções de Javascript e falhas de renderização de componentes da interface.
*   `frontend.route_change`: Mudança de página ou tab por parte do usuário.

---

## 3. Coleta de Logs do Frontend (Ingestion Endpoint)

Como o frontend roda no navegador do cliente final, ele não pode escrever logs diretamente no disco do servidor. A estratégia prevê:

1.  **Endpoint de Ingestão de Logs**: O backend disponibilizará uma rota HTTP dedicada de escrita rápida (ex: `/api/v1/observability/logs`).
2.  **Envio em Lotes**: O frontend agrupará logs gerados no navegador e enviará via POST em lotes estruturados sob o mesmo schema JSON para esse endpoint.
3.  **Escrita Unificada**: O endpoint de ingestão validará a estrutura do JSON recebido e chamará o dispatcher local para gravar os logs diretamente no arquivo `.jsonl` do servidor, garantindo centralização absoluta.
