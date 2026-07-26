# Catálogo de Categorias e Eventos

Este guia consolida todas as categorias e eventos suportados pelo sistema de logging, definindo regras claras de emissão.

## 1. Categorias Oficiais

O SimCC adota cinco categorias principais de log:

*   **`http`**: Relativo ao ciclo de vida de chamadas de rede da API.
*   **`database`**: Logs emitidos por operações com banco de dados.
*   **`routine`**: Logs de scripts em lote executados em background ou via CLI (Hop/scripts).
*   **`frontend`**: Logs previstos para ações iniciadas na interface do usuário (UI).
*   **`system`**: Logs gerais de inicialização de serviços, healthchecks e configurações globais.

---

## 2. Catálogo de Eventos por Categoria

| Categoria | Nome do Evento | Descrição | Emissor Recomendado |
| :--- | :--- | :--- | :--- |
| **`http`** | `request.received` | Requisição HTTP recebida na API | Middleware |
| **`http`** | `request.finished` | Resposta HTTP enviada com sucesso | Middleware |
| **`http`** | `request.error` | Requisição HTTP falhou (HTTP 5XX ou exceção) | Middleware |
| **`database`**| `query.error` | Uma instrução SQL ou transação de banco falhou | Event Listener |
| **`routine`** | `routine.started` | Execução de rotina CLI em lote iniciada | Wrapper (`run_routine.py`) |
| **`routine`** | `routine.finished`| Execução de rotina em lote concluída com sucesso | Wrapper (`run_routine.py`) |
| **`routine`** | `routine.error` | Rotina em lote falhou (exceção lançada) | Wrapper (`run_routine.py`) |

---

## 3. Fluxos de Eventos Esperados

### A. Fluxo HTTP de Sucesso
```
HTTP request.received  ──>  HTTP request.finished
```

### B. Fluxo HTTP com Erro Interno (Falha em Banco)
```
HTTP request.received  ──>  DATABASE query.error  ──>  HTTP request.error
```

### C. Fluxo de Rotina CLI com Sucesso
```
ROUTINE routine.started  ──>  ROUTINE routine.finished
```

### D. Fluxo de Rotina CLI com Falha
```
ROUTINE routine.started  ──>  ROUTINE routine.error
```

---

## 4. Diretrizes: Quando Utilizar e Quando NÃO Utilizar

*   **NÃO declare strings de eventos livremente**: Nunca faça `logger.info("request.finished")` solto no código. Chame sempre a função helper correspondente no arquivo `events.py` (ex. `request_finished()`).
*   **Não polua logs de banco com SQLs em produção**: logs de banco de dados devem registrar apenas falhas (`query.error`). O comando SQL e parâmetros **nunca** devem ser persistidos em produção para evitar vazamento de dados confidenciais ou sobrecarga do disco.
*   **Foco das camadas**:
    *   Routers/Controllers **não** emitem logs de banco de dados ou rotinas.
    *   Repositories e Query classes **não** conhecem transações HTTP.
    *   Mantenha cada camada emitindo eventos sob sua área de responsabilidade.

## 5. Convenção para Novos Eventos

Caso haja necessidade real de criar um novo evento:
1.  **Escolha a Categoria**: Enquadre na categoria correta. Se não existir, avalie a necessidade de estender as categorias em `constants.py`.
2.  **Defina a Nomenclatura**: Use a convenção `<entidade>.<acao>` em caixa baixa (ex: `auth.login`, `file.uploaded`).
3.  **Adicione a Constants**: Registre o novo evento em `LogEvent` (`constants.py`).
4.  **Crie a Função Helper**: Desenvolva a função correspondente em `events.py` tipando corretamente os parâmetros para que o código de negócio chame apenas a função helper.
